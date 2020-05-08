package com.hongkuncc.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark02_RDD2 {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf对象
    val sparkConf = new SparkConf().setAppName("Spark02_RDD1").setMaster("local[*]")
    // 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    // 3. 构建RDD: 1.从内存中创建
    //并行
    //1.parallelize
    //val rdd:RDD[Int] = sc.parallelize(List(1,2,3,4))

    //2.makeRDD,底层代码调用parallelize
  //val rdd1:RDD[Int] =sc.makeRDD(List(1,2,3,4))
  //在创建RDD的同时，指定数据分区的数量，其实就是设定第二个参数
  val lineRDD:RDD[String] =sc.textFile("input/1.txt")


  //3.将数据保存到指定路径中
    //保存文件时，会按照默认的分区数量进行保存
  lineRDD.saveAsTextFile("output")

    //释放连接
    // sc.stop()

}

}
