package com.hongkuncc.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark07_RDD_Transform5 {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf对象
    val sparkConf = new SparkConf().setAppName("Spark07_RDD_Transform2").setMaster("local[*]")
    // 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    // 3. 构建RDD
    val numRDD:RDD[Int] = sc.makeRDD(Array(10,20,30,40,50,60,70,80),numSlices = 2)

    // 获取每个分区中的最大值后求和
    val array = Array(List(10,20),List(30,40),List(50,60),List(70,80))
    val ints:Array[Int] = array.map(list =>list.max)
    print(ints.sum)


    val glomRDD:RDD[Array[Int]] = numRDD.glom()
    val partitionMaxRDD: RDD[Int] = glomRDD.map(array=>array.max)
    print(partitionMaxRDD.sum().toInt)


    //释放连接
    sc.stop()
  }

}
