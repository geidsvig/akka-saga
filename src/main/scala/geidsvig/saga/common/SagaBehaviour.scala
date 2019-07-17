package geidsvig.saga.common

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import geidsvig.logger.Slf4jLogger
import geidsvig.saga.common.Saga.SagaTransaction
import geidsvig.saga.common.SagaEvents.SagaEvent
import geidsvig.saga.common.SagaFailureBehaviours.{SagaStepFailureIgnore, SagaStepForward, SagaStepRetry}
import geidsvig.saga.common.SagaStepResponses.{SagaFailureResponse, SagaSuccessResponse}

import scala.concurrent.Future
import scala.util.{Try, _}

/**
  * <b>The Saga Pattern</b> - Divide long-lived distributed transactions into quick
  * local ones with compensating actions for recovery.
  * <p>
  * In a distributed environment, achieving consistency is difficult.
  * Transactions cannot rely on the traditional ACID relational db approach.
  * <p>
  * Many projects rely heavily on actors as services, so this Saga solution provides a way to describe
  * <b>steps</b> as service message <b>actions</b> and <b>compensating actions</b>. Each step also defines
  * the behaviour to be used if the <b>action</b> fails.
  * <p>
  * <b>Important:</b> Each step is idempotent, and has no dependency on any steps before it. Given actions T1, T2,
  * and compensating actions C1, C2:
  * <ul>
  *   <li>Cn semantically undoes Tn</li>
  *   <li>T2 does not depend on the state nor result of T1</li>
  *   <li>C1 does not depend on the state nor result of C2</li>
  * </ul>
  * <p>
  * Default behaviour provided by this trait does not include persistence, nor recovery of a Saga transaction.
  * However, the hooks are provided for the implementation of a durable Saga transaction.
  * <p>
  * When configured with a [[SagaTransaction]] and a map of services, the
  * saga pattern can be executed with startSaga()</p>
  * <p>
  * Using this trait, one must provide a cancel() implementation
  *
  * @author Garrett Eidsvig
  *
  */
trait SagaBehaviour {
  this: Slf4jLogger =>

  import scala.concurrent.ExecutionContext.Implicits.global

  val sagaName: String
  val sagaTransaction: SagaTransaction
  val services: Map[String,ActorRef]

  var rollback = false

  /**
    * It is left to the caller of this function to know the expected result and
    * cast to that expected result.
    *
    * @return the result of the last completed action or compensating action
    */
  def run(): Future[Try[Any]] = {
    record(SagaEvents.Started(sagaTransaction))
    nextStep(0, 0)
  }

  /**
    * Lookup the service actorRef in the services map by the provided serviceName.
    *
    * @param serviceName
    * @return
    */
  private def lookupService(serviceName: String): Future[ActorRef] = {
    services.get(serviceName) match {
      case Some(service) => Future.successful(service)
      case None => Future.failed(new Exception(s"service ${serviceName} not defined"))
    }
  }

  /**
    * Handle the next action in the Saga sequence of steps.
    *
    * @param currentStep
    * @param attemptCounter
    * @return
    */
  def nextStep(currentStep: Int, attemptCounter: Int): Future[Try[Any]] = {

    record(SagaEvents.StepStarted(currentStep))

    def handleSuccess(result: Any): Future[Try[Any]] = {
      if (currentStep < sagaTransaction.steps.size-1) {
        record(SagaEvents.StepCompleted(currentStep))
        nextStep(currentStep + 1, 0)
      } else {
        record(SagaEvents.Completed(Some(result)))
        Future.successful(Success(result))
      }
    }

    def handleFailure(t: Throwable): Future[Try[Any]] = {
      record(SagaEvents.StepFailed(currentStep))
      sagaTransaction.steps.get(currentStep) match {
        case Some(step) => step.failureBehavior match {
          case SagaStepForward => forwardRecovery()
          case SagaStepFailureIgnore =>
            nextStep(currentStep + 1, 0)
          case SagaStepRetry(n) if attemptCounter < n =>
            nextStep(currentStep, attemptCounter + 1)
          case _ =>
            compensate(currentStep - 1, 0)
            //return Failure with waiting compensate completed.
            Future.successful(Failure(t))
        }
        case _ => Future.failed(new Error("impossible to recover from no current step..."))
      }
    }

    //logic starts here
    val actionResult = for {
      step <- sagaTransaction.steps.get(currentStep)
      service <- services.get(step.action.serviceName)
    } yield {
      sagaAsk(service, step.action.message, step.action.timeout).flatMap {
        case Success(result) => handleSuccess(result)
        case Failure(t) => handleFailure(t)
      }.recoverWith { case t => handleFailure(t) }
    }


    actionResult match {
      case None => Future.successful(Failure(new Exception(s"Failed at ${currentStep}")))
      case Some(r) => r
    }
  }

  /**
    * Applying compensation to the step
    *
    * @param currentStep
    * @param attemptCounter
    * @return
    */
  def compensate(currentStep: Int, attemptCounter: Int): Future[Try[Any]] = {
    def skipCompensation(): Future[Try[Any]] = {
      record(SagaEvents.StepCompensationSucceeded(currentStep))
      compensate(currentStep - 1, 0)
    }

    def handleSuccess(result: Any): Future[Try[Any]] = {
      record(SagaEvents.StepCompensationSucceeded(currentStep))
      compensate(currentStep - 1, 0)
    }

    def handleFailure(t: Throwable): Future[Try[Any]] = {
      record(SagaEvents.StepCompensationFailed(currentStep))
      Future.successful(Failure(new Exception(s"saga failed to roll back at step $currentStep. Cause: ${t.getMessage}")))
    }

    if (currentStep < 0) {
      record(SagaEvents.Completed(None))
      Future.successful(Failure(new Exception("saga rolled back")))
    } else {
      record(SagaEvents.StepCompensationStarted(currentStep))

      val compensationResult = for {
        sagaStep <- sagaTransaction.steps.get(currentStep)
        compensation <- sagaStep.compensatingAction
        service <- services.get(compensation.serviceName)
      } yield {
        sagaAsk(service, compensation.message, compensation.timeout).flatMap {
          case Success(result) => handleSuccess(result)
          case Failure(t) => handleFailure(t)
        }.recoverWith { case t => handleFailure(t) }
      }

      compensationResult match {
        case None => skipCompensation()
        case Some(r) => r
      }
    }
  }

  /**
    * Theoretically, a Saga may defer the completion of the saga to a later time. Retrying when a
    * dependent service becomes available again.
    *
    * @return
    */
  def forwardRecovery() = Future.failed(new Exception("saga step forward recovery not supported"))

  /**
    * Override with whatever logging, persistent actor, etc logic you desire.
    *
    * @param event
    */
  def record(event: SagaEvent): Unit = {
    logger.debug(s"$sagaName $event")
  }

  /**
    * Maps response from service to [[Try]].
    *
    * @param ref
    * @param message
    * @param timeout
    * @param sender
    * @return
    */
  def sagaAsk(ref: ActorRef, message:Any, timeout:Timeout)(implicit sender:ActorRef = ActorRef.noSender):Future[Try[Any]] = {
    ask(ref, message)(timeout)
      .map {
        case result: SagaSuccessResponse => Success(result)
        case result: SagaFailureResponse => Failure(new Exception(s"failure response $result"))
        case result: Try[Any]  => result  //some service respond with Try
        case result => Failure(new Exception(s"unsupported saga response $result"))
      }
      .recover {
        case t => Failure(t)
      }
  }

}
