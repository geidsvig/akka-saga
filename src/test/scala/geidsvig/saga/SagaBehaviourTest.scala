package geidsvig.saga

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.TestActorRef
import akka.util.Timeout
import geidsvig.saga.common.Saga._
import geidsvig.saga.common.SagaFailureBehaviours.{SagaStepFailureIgnore, SagaStepRetry}
import geidsvig.saga.common.SagaStepResponses._
import geidsvig.saga.common._
import SagaTestService._
import geidsvig.saga.common.Saga.SagaAction
import geidsvig.test.TestKitSpec
import geidsvig.util.RespondsWith
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._
import scala.util._

/**
  * Created by garretteidsvig on 2019-07-16.
  */
class SagaBehaviourTest extends TestKitSpec(ActorSystem("SagaBehaviourTestSystem")) with ScalaFutures {

  import scala.concurrent.ExecutionContext.Implicits.global
  implicit val timeout = Timeout(3 seconds)

  val testService = TestActorRef(Props(new SagaTestService()))
  val testServiceName = "testService"
  val services:Map[String, ActorRef] = Map(testServiceName -> testService)

  val successAction = SagaAction(testServiceName, RespondWithSuccess(), Timeout(2 seconds))
  val failureAction = SagaAction(testServiceName, RespondWithFailure())
  val failOnceAction = SagaAction(testServiceName, RespondWithFailureOnce())

  val successStep = SagaStep(successAction, Some(successAction), SagaStepFailureIgnore)
  val failureStep = SagaStep(failureAction, Some(successAction), SagaStepRetry(0))
  val failOnceStep = SagaStep(failOnceAction, Some(successAction), SagaStepRetry(1))

  "Saga Behaviour" should "handle a multi step saga successfully" in {
    Given("a saga with successful steps")
    val sagaTransaction: SagaTransaction = SagaTransaction(Map(
      0 -> successStep,
      1 -> successStep,
      2 -> successStep
    ))
    val saga = new Saga("happy-path-saga", sagaTransaction, services)
    When("running the saga")
    Then("the saga should complete all steps")
    whenReady(
      for {
        result <- saga.run()
      } yield result
    )( sagaResult => sagaResult match {
      case Success(SagaSuccessTestResponse(result)) => assert(result)
      case _ => assert(false)
    })
  }

  "Saga Behaviour" should "rollback a multi step saga when a step fails" in {
    Given("a saga with a failing step")
    val sagaTransaction: SagaTransaction = SagaTransaction(Map(
      0 -> successStep,
      1 -> successStep,
      2 -> failureStep,
      3 -> successStep
    ))
    val saga = new Saga("compensating-saga", sagaTransaction, services)
    When("running the saga")
    Then("the saga should fail on a step and rollback all previous steps")
    whenReady(
      (for {
        result <- saga.run()
      } yield result)
        .recover {
          case e => e
        }
    ) { sagaResult => sagaResult match {
      case Failure(e: Throwable) => assert(true)
      case _ => assert(false)
    }}
  }

  "Saga Behaviour" should "retry a step when a step fails" in {
    Given("a saga with a failing step")
    val sagaTransaction: SagaTransaction = SagaTransaction(Map(
      0 -> successStep,
      1 -> successStep,
      2 -> failOnceStep,
      3 -> successStep
    ))
    val saga = new Saga("retrying-saga", sagaTransaction, services)
    When("running the saga")
    Then("the saga should fail on a step and retry before completing all steps")
    whenReady(
      for {
        result <- saga.run()
      } yield result
    )( sagaResult => sagaResult match {
      case Success(SagaSuccessTestResponse(result)) => assert(result)
      case _ => assert(false)
    })
  }

}

object SagaTestService {
  case class RespondWithSuccess() extends RespondsWith[Try[Any]]
  case class RespondWithFailure() extends RespondsWith[Try[Any]]
  case class RespondWithFailureOnce() extends RespondsWith[Try[Any]]

  case class SagaSuccessTestResponse(result: Boolean) extends SagaSuccessResponse
  case class SagaFailureTestResponse(t: Throwable) extends SagaFailureResponse
}

class SagaTestService() extends Actor {
  override def receive = {
    case RespondWithSuccess() => sender ! SagaSuccessTestResponse(true)
    case RespondWithFailure() => sender ! SagaFailureTestResponse(new Error("failure"))
    case RespondWithFailureOnce() => {
      sender ! SagaFailureTestResponse(new Error("failure"))
      context.become(alwaysSuccessful)
    }
  }
  def alwaysSuccessful:Actor.Receive = {
    case RespondWithSuccess() => sender ! SagaSuccessTestResponse(true)
    case RespondWithFailure() => sender ! SagaSuccessTestResponse(true)
    case RespondWithFailureOnce() => sender ! SagaSuccessTestResponse(true)
  }
}

