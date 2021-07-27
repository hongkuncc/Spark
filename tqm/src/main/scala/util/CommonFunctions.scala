package util

import org.slf4j.{Logger, LoggerFactory}



object CommonFunctions {

  //设置日志打印级别为 info
  val logger = LoggerFactory.getLogger(this.getClass)
//  Logger.getLogge("").setLevel(Level.INFO)
  def printInfo(info: String): Unit = {
    logger.info(NowDate(sdf) + "*****" + info +"***********")
  }

}
