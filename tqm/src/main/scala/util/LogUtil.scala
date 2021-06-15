package com.hongkuncc.bigdata.spark.Util

import org.apache.log4j.{Level, Logger}
import org.slf4j.LoggerFactory

object LogUtil {
  /*
  * 日志打印
  *
  *
  * */
def getLogger(jobName: String):org.slf4j.Logger={
  val logger = LoggerFactory.getLogger(jobName)
  Logger.getLogger(jobName).setLevel(Level.INFO)
  logger
}

}
