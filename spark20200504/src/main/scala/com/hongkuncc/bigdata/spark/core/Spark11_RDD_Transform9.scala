package com.hongkuncc.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark11_RDD_Transform9 {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf对象
    val sparkConf = new SparkConf().setAppName("Spark07_RDD_Transform2").setMaster("local[*]")
    // 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    // 3. 构建RDD
    val numRDD:RDD[Int] = sc.parallelize(1 to 10,5)
    val array: Array[Array[Int]] = numRDD.glom().collect()
    array.foreach(a =>println(a.mkString(",")))
    //println(numRDD.glom().collect().mkString(","))

   /* mapPartitions(func)
    作用: 类似于map(func), 但是是独立在每个分区上运行.所以:Iterator<T> => Iterator<U>
      假设有N个元素，有M个分区，那么map的函数的将被调用N次,而mapPartitions被调用M次,一个函数一次处理所有分区。
    */

    //    print(numRDD.partitions.length)

    //合并分区:1)不shuffle，2）有shuffle
    val coalesceRDD: RDD[Int] = numRDD.coalesce(2)
    val array2: Array[Array[Int]] = coalesceRDD.glom().collect()
    array2.foreach(b=>println(b.mkString(",")))
    //println(coalesceRDD.glom().collect().mkString(","))

    //释放连接
    sc.stop()
  }

}
