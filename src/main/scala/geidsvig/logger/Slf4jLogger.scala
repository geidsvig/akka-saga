package geidsvig.logger

import com.typesafe.scalalogging.Logger

/**
  * Created by garretteidsvig on 2019-07-16.
  */
object Slf4jLogger {
  def getLogger(cls: Class[_]) = Logger(cls)
}

trait Slf4jLogger {
  val logger = Logger(getClass)
}
