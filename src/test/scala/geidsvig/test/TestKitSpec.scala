package geidsvig.test

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest._

/**
  * Created by garretteidsvig on 2019-07-16.
  */
abstract class TestKitSpec(system: ActorSystem)
  extends TestKit(system)
    with FlatSpecLike
    with Matchers
    with GivenWhenThen
    with BeforeAndAfterAll
    with ImplicitSender {

  override def afterAll(): Unit = {
    system.terminate()
  }

}
