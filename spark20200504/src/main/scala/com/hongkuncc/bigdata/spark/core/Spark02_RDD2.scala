package com.hongkuncc.bigdata.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Spark02_RDD_Transform1 {
  def main(args: Array[String]): Unit = {
     // 1. 创建 SparkConf对象
    val sparkConf = new SparkConf().setAppName("Spark02_RDD_Transform1").setMaster("local[*]")
    // 2. 创建Spark环境连接对象
    val sc = new SparkConf(sparkConf)

    //构建RDD
    val nullRDD:RDD[Int] = sc.makeRDD(List(1,2,3,4))

    // 将RDD通过算子进行转换，变成新的RDD
    //



  }

}
