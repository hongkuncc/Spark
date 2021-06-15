package com.hongkuncc.bigdata.spark.Util

import org.apache.spark.sql.SparkSession

object HiveUtil {
  def setMaxpartitions(spark:SparkSession): Unit ={
    spark.sql("set hive.exec.dynamic.partition=true")
  }

}
