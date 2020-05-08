package com.hongkuncc.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark02_RDD_Transform3 {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf对象
    val sparkConf = new SparkConf().setAppName("Spark02_RDD_Transform2").setMaster("local[*]")
    // 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    // 3. 构建RDD
    val numRDD:RDD[Int] = sc.makeRDD(List(1,2,3,4),numSlices = 2)

    // 将RDD通过算子进行转换，变成新的RDD
    //转换算子 - mapPartitions
    val mapPartitionsWithIndexRDD:RDD[(Int,Int)] = numRDD.mapPartitionsWithIndex(
      (index,datas) =>{
        datas.map(
          data => (index,data)
        )
      })
    val collect: Array[(Int,Int)] = mapPartitionsWithIndexRDD.collect()
    val tuples:Array[(Int,Int)] = collect.filter(t => t._1 == 1)
    print(tuples.map(_._2).sum)

    //释放连接
    sc.stop()
  }

}
