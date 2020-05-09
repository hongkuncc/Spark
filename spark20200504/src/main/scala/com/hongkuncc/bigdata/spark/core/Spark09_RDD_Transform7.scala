package com.hongkuncc.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark09_RDD_Transform7 {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf对象
    val sparkConf = new SparkConf().setAppName("Spark07_RDD_Transform2").setMaster("local[*]")
    // 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    // 3. 构建RDD
    val numRDD:RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10))

    //抽样算子sample
    //4.随机抽取数据
    val sampleRDD: RDD[Int] = numRDD.sample(withReplacement = false,0.5)

    print(sampleRDD.collect().mkString(","))

    //释放连接
    sc.stop()
  }

}
