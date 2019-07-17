package geidsvig.saga.common

import akka.actor.ActorRef
import akka.util.Timeout
import Saga.SagaTransaction
import geidsvig.logger.Slf4jLogger

import scala.concurrent.duration._

/**
  * Building blocks of the Saga pattern.
  */
object Saga {
  case class SagaAction(serviceName: String, message: Any, timeout: Timeout = Timeout(2000 milliseconds))
  case class SagaStep(action: SagaAction, compensatingAction: Option[SagaAction], failureBehavior: SagaFailureBehaviours.SagaFailureBehaviour)
  case class SagaTransaction(steps: Map[Int, SagaStep])
}

/**
  * A general purpose solution for the Saga pattern.
  *
  * @param sagaName
  * @param sagaTransaction
  * @param services
  */
class Saga(
  override val sagaName: String,
  override val sagaTransaction: SagaTransaction,
  override val services: Map[String,ActorRef]
)
  extends SagaBehaviour
  with Slf4jLogger

/**
  * Events for each step outcome of the Saga.
  */
object SagaEvents {
  sealed trait SagaEvent
  case class Started(sagaTransaction: SagaTransaction) extends SagaEvent
  case class StepStarted(stepIndex: Int) extends SagaEvent
  case class StepCompleted(stepIndex: Int) extends SagaEvent
  case class StepRetried(stepIndex: Int, retryCount: Int) extends SagaEvent
  case class StepFailed(stepIndex: Int) extends SagaEvent
  case class StepCompensationStarted(stepIndex: Int) extends SagaEvent
  case class StepCompensationSucceeded(stepIndex: Int) extends SagaEvent
  case class StepCompensationFailed(stepIndex: Int) extends SagaEvent

  /**
    *
    * @param result Some value when all steps complete sucessfully. None when saga is rolled back.
    */
  case class Completed(result: Option[Any]) extends SagaEvent
}

/**
  * Behaviours for saga step failure.
  */
object SagaFailureBehaviours {
  sealed trait SagaFailureBehaviour
  //n defines the max retry time. If it is zero, means no retry and run compensate
  case class SagaStepRetry(n:Int) extends SagaFailureBehaviour
  case object SagaStepFailureIgnore extends SagaFailureBehaviour
  //for continuing an incompleted saga in the future.
  case object SagaStepForward extends SagaFailureBehaviour
}

object SagaStepResponses {
  sealed trait SagaStepResponse
  trait SagaSuccessResponse extends SagaStepResponse
  trait SagaFailureResponse extends SagaStepResponse
}
