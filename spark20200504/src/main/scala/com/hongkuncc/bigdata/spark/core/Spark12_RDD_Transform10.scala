package com.hongkuncc.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark12_RDD_Transform10 {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf对象
    val sparkConf = new SparkConf().setAppName("Spark07_RDD_Transform2").setMaster("local[*]")
    // 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    // 3. 构建RDD
    val numRDD:RDD[Int] = sc.makeRDD(List(2,5,8,3,9,4,2))

    val sortRDD: RDD[Int] = numRDD.sortBy(num=>num,false)
    println(sortRDD.collect().mkString(","))


    //释放连接
    sc.stop()
  }

}
