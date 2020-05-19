package com.hongkuncc.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark27_RDD_Action5 {

  def main(args: Array[String]): Unit = {

    // TODO 1. 创建Spark配置对象
    val sparkConf = new SparkConf().setAppName("Spark08_RDD_Transform5").setMaster("local[*]")

    // TODO 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    // TODO 3. 构建RDD
    val rdd = sc.makeRDD(List(1,2,3,4,5,6))

    //rdd.collect().foreach(println)

    // 算子外部的代码在Driver端执行
    // 算子内部的代码在Executor端执行

    rdd.foreach(println)

    // TODO 9. 释放连接
    sc.stop()
  }
}
