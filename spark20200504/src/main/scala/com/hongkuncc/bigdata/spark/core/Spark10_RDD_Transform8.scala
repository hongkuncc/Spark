package com.hongkuncc.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark10_RDD_Transform8 {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf对象
    val sparkConf = new SparkConf().setAppName("Spark07_RDD_Transform2").setMaster("local[*]")
    // 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    // 3. 构建RDD
    val numRDD:RDD[Int] = sc.makeRDD(List(1,1,2,2,3,3,4,4),2)

    //distinct


    print(numRDD.distinct().collect().mkString(","))

    //释放连接
    sc.stop()
  }

}
