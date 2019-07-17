package geidsvig.saga

import akka.actor.ActorRef
import akka.persistence.{PersistentActor, RecoveryCompleted}
import geidsvig.logger.Slf4jLogger
import geidsvig.saga.common.Saga.SagaTransaction
import geidsvig.saga.common.SagaBehaviour
import geidsvig.saga.common.SagaEvents._

import scala.concurrent.Future


/**
  *
  * @param sagaName the unique identifier with meaningful saga name. ex. "transfer-funds-<playerId>-<requestId>"
  * @param sagaTransaction
  * @param services service actorRefs used for the saga steps and for storing the sender ref.
  * @param senderName the key for the service in services map to send the result to.
  */
class SagaPersistentActor(override val sagaName: String,
                          override val sagaTransaction: SagaTransaction,
                          override val services: Map[String, ActorRef],
                          val senderName: String
                         )
  extends PersistentActor
  with SagaBehaviour
  with Slf4jLogger {

  import scala.concurrent.ExecutionContext.Implicits.global

  override def persistenceId: String = sagaName

  var currentStep = 0
  var stepRetries = 0
  var isCompensating = false
  var result: Any = null

  override def receiveCommand = {
    case unhandled => logger.warn(s"unhandled receive message $unhandled")
  }

  override def receiveRecover = {
    case Started(sagaTransaction) => {
      logger.debug(s"recovering Started($sagaTransaction)")
      //TODO check each step for the same step messages.
      // verify that the saga originally started, and persisted, matches the retried transaction.
      assert(this.sagaTransaction.steps.size == sagaTransaction.steps.size)
    }
    case StepStarted(stepIndex) => {
      logger.debug(s"recovering StepStarted($stepIndex)")
      currentStep = stepIndex
      stepRetries = 0
    }
    case StepCompleted(stepIndex) => {
      logger.debug(s"recovering StepCompleted($stepIndex)")
      currentStep = stepIndex + 1
    }
    case StepRetried(stepIndex, retryCount) => {
      logger.debug(s"recovering StepRetried($stepIndex, $retryCount)")
      isCompensating = true
      currentStep = stepIndex
      stepRetries = retryCount
    }
    case StepFailed(stepIndex) => {
      logger.debug(s"recovering StepFailed($stepIndex)")
      isCompensating = true
      currentStep = stepIndex
    }
    case StepCompensationStarted(stepIndex) => {
      logger.debug(s"recovering StepCompensationStarted($stepIndex)")
      isCompensating = true
      currentStep = stepIndex
      stepRetries = 0
    }
    case StepCompensationSucceeded(stepIndex) => {
      logger.debug(s"recovering StepCompensationSucceeded($stepIndex)")
      isCompensating = true
      currentStep = stepIndex - 1
      stepRetries = 0
    }
    case StepCompensationFailed(stepIndex) => {
      logger.debug("recovering StepCompensationFailed. aborting recovery. cannot deal with failed compensation.")
      cancel()
    }
    case Completed(result) => {
      logger.debug(s"recovering Completed($result)")
      this.result = result
    }

    case RecoveryCompleted => {
      logger.debug("RecoveryCompleted")
      runSaga()
    }

    case unhandled => logger.warn(s"unhandled recovery message $unhandled")
  }

  def cancel() = context.stop(self)

  /**
    * Overriding onPersistFailure so that we can log the failure before the actor dies.
    *
    * @param cause
    * @param event
    * @param seqNr
    */
  override def onPersistFailure(cause: Throwable, event: Any, seqNr: Long): Unit = {
    logger.error(s"persistence failure from $event ... cased by $cause")
    //the actor will now die...
    super.onPersistFailure(cause, event, seqNr)
  }

  def runSaga(): Unit = {
    if (result != null) Future.successful(result)
    else if (isCompensating) compensate(currentStep, stepRetries)
    else nextStep(currentStep, stepRetries)
      .map ( result => {
        logger.debug(s"Saga completed. sending result: $result to $senderName")
        services(senderName) ! result
      })
  }

  /**
    * Persists event.
    *
    * @param event
    */
  override def record(event: SagaEvent): Unit = {
    persist(event){ event => {
      logger.debug(s"$sagaName persisted $event")
    }}
  }

}
