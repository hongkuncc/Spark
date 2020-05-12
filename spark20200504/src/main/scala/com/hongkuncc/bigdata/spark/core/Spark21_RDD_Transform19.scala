package com.hongkuncc.bigdata.spark.core


import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark21_RDD_Transform19 {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf对象
    val sparkConf = new SparkConf().setAppName("Spark07_RDD_Transform2").setMaster("local[*]")
    // 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    // 3. 从日志文件获取原始数据

    // 4.将数据进行分解，保留业务需要的数据

    // 5.将业务需要的数据的结构进行改变

    // 6.将转换结构后的数据
    // 7.将业务需要的数据的结构进行改变
    // 8.将业务需要的数据的结构进行改变
    //

    val rdd = sc.parallelize(Array((1, "a"), (10, "b"), (11, "c"), (4, "d"), (20, "d"), (10, "e")))


    //释放连接
    sc.stop()
  }

}