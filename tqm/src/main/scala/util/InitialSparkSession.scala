package com.hongkuncc.bigdata.spark.Util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class InitialSparkSession extends Serializable {
  def  getSparkSession(jobName: String): SparkSession = {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.sql.inMemoryColumnarStorage.compressed","true")
      .set("spark.sql.inMemoryColumnarStorage.batchSize","20000")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .set("spark.debug.maxToStringFields","1000")
      .set("spark.sql.parquet.writeLegacyFormat","true")

    val sparkSession = SparkSession.builder()
        .appName(jobName)
        .config(sparkConf)
        .enableHiveSupport()
        .getOrCreate()

   sparkSession
  }

}
