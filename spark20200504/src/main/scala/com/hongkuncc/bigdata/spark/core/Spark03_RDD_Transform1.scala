package com.hongkuncc.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark03_RDD_Transform1 {
  def main(args: Array[String]): Unit = {
     // 1. 创建 SparkConf对象
    val sparkConf = new SparkConf().setAppName("Spark03_RDD_Transform1").setMaster("local[*]")
    // 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    // 3. 构建RDD
    val numRDD:RDD[Int] = sc.makeRDD(List(1,2,3,4),numSlices = 2)

    // 将RDD通过算子进行转换，变成新的RDD
    //转换算子 - map
    val mapRDD:RDD[Int] = numRDD.map(num =>{num*2})
    //mapRDD.collect().foreach(println)
    mapRDD.saveAsTextFile("output")

    //释放连接
    sc.stop()
  }

}
