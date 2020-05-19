package com.hongkuncc.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark26_RDD_Action4 {

  def main(args: Array[String]): Unit = {

    // TODO 1. 创建Spark配置对象
    val sparkConf = new SparkConf().setAppName("Spark08_RDD_Transform5").setMaster("local[*]")

    // TODO 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    // TODO 3. 构建RDD
    val rdd1 = sc.makeRDD(List(("a",1), ("b",1),("c",1),("a",1), ("b",1),("c",1)))
    val rdd2 = sc.makeRDD(List(("a",1), ("b",1),("c",1),("a",2), ("b",3),("c",4)))
    val rdd3 = sc.makeRDD(List("a","a","a","a","b","b"))

    // 7. WordCount
    val stringToLong: collection.Map[String, Long] = rdd2.countByKey()

    // 8. WordCount
    val stringToLong1: collection.Map[String, Long] = rdd3.countByValue()

    stringToLong1.foreach(println)

    // TODO 9. 释放连接
    sc.stop()
  }
}
