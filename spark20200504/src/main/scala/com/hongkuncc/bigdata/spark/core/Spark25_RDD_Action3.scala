package com.hongkuncc.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark25_RDD_Action3 {

  def main(args: Array[String]): Unit = {

    // TODO 1. 创建Spark配置对象
    val sparkConf = new SparkConf().setAppName("Spark08_RDD_Transform5").setMaster("local[*]")

    // TODO 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    // TODO 3. 构建RDD
    val rdd1 = sc.makeRDD(Array(1,2,3,4), 2)

    //rdd1.saveAsTextFile("output")
   // rdd1.saveAsObjectFile("output1")
    //rdd1.map((_,1)).saveAsSequenceFile("output2")

    // TODO 9. 释放连接
    sc.stop()
  }
}
