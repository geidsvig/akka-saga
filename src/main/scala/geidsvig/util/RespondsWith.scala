package geidsvig.util

import scala.concurrent.{ExecutionContext, Future}

/**
  *
  * Useful for readability and compile time checking that the response type is the expected one after refactoring.
  */
trait RespondsWith[T] {
  type ResponseType = T
  // A static type checking proxy for calling handler(req). Lets the compiler help you ensure that handler actually responds with the declared type.
  def handleWith(func: this.type => Future[T])(implicit ec:ExecutionContext):Future[T] = func(this)
}
