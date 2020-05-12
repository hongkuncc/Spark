package com.hongkuncc.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark24_RDD_Action2 {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf对象
    val sparkConf = new SparkConf().setAppName("Spark07_RDD_Transform2").setMaster("local[*]")
    // 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    // 3. 构建RDD
    val rdd1 = sc.makeRDD(Array(1,2,3,4))


    sc.stop()
  }
}
