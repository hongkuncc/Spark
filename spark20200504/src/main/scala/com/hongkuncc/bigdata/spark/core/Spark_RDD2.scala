package com.hongkuncc.bigdata.spark.core

import org.apache.spark.SparkConf

object Spark_RDD2 {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf对象
    val sparkConf = new SparkConf().setAppName("Spark02_RDD1").setMaster("local[*]")
    // 2. 创建Spark环境连接对象


    // 3. 构建RDD

  }

}
