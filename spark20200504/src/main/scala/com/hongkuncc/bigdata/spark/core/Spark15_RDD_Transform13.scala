package com.hongkuncc.bigdata.spark.core


import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark15_RDD_Transform13 {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf对象
    val sparkConf = new SparkConf().setAppName("Spark07_RDD_Transform2").setMaster("local[*]")
    // 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    // 3. 构建RDD
    val rdd = sc.makeRDD(List(("a",1),("b",2),("c",3),("a",1),("b",2),("c",3)),3)

    //reduceByKey
    val reduceByKeyRDD: RDD[(String, Int)] = rdd.reduceByKey((x, y)=>(x+y))

    println(reduceByKeyRDD.collect().mkString(","))

    //释放连接
    sc.stop()
  }

}