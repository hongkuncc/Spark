package com.hongkuncc.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark08_RDD_Transform6 {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf对象
    val sparkConf = new SparkConf().setAppName("Spark07_RDD_Transform2").setMaster("local[*]")
    // 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    // 3. 构建RDD
    val numRDD:RDD[Int] = sc.makeRDD(List(1,2,3,4))

    //转换算子group-by
    //4.根据奇偶数进行分组
    //val groupByRDD: RDD[(Int, Iterable[Int])] = numRDD.groupBy(num=>num%2)
    //print(groupByRDD.collect().mkString(","))


    //转换算子filter
    val filterRDD: RDD[Int] = numRDD.filter(num=>num%2!=0)
    print(filterRDD.collect().mkString(","))

    //释放连接
    sc.stop()
  }

}
