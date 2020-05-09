package com.hongkuncc.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark06_RDD_Transform4 {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf对象
    val sparkConf = new SparkConf().setAppName("Spark06_RDD_Transform2").setMaster("local[*]")
    // 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    // 3. 构建RDD
    val numRDD:RDD[Int] = sc.makeRDD(List(1,2,3,4),numSlices = 2)

    // 将集合中的每一个数据进行结构的转换，并形成平方和立方的组合
    //转换算子 - flatmap
    val flatMapRDD:RDD[Int] = numRDD.flatMap(num=>{List(num*num,num*num*num)})
    print(flatMapRDD.collect.mkString(","))

    //释放连接
    sc.stop()
  }

}
