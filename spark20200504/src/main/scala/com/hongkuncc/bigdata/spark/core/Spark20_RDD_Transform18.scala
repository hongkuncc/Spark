package com.hongkuncc.bigdata.spark.core


import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark20_RDD_Transform18 {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf对象
    val sparkConf = new SparkConf().setAppName("Spark07_RDD_Transform2").setMaster("local[*]")
    // 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    // 3. 构建RDD
    /*val rdd = sc.parallelize(Array((1, "a"), (10, "b"), (11, "c"), (4, "d"), (20, "d"), (10, "e")))
    //combineByKey
    val sortRDD: RDD[(Int, String)] = rdd.sortByKey(false)

    println(sortRDD.collect().mkString(","))

    val mapValuesRDD: RDD[(Int, String)] = rdd.mapValues("<" + _ + ">")
    println(mapValuesRDD.collect().mkString(","))*/


    var rdd1 = sc.parallelize(Array((1, "a"), (1, "b"), (2, "c")))
    var rdd2 = sc.parallelize(Array((1, "aa"), (3, "bb"), (2, "cc")))
   /* val joinRDD: Array[(Int, (String, String))] = rdd1.join(rdd2).collect()
    joinRDD.foreach(println)*/

    val cogroupRDD: Array[(Int, (Iterable[String], Iterable[String]))] = rdd1.cogroup(rdd2).collect()
    cogroupRDD.foreach(println)

    //释放连接
    sc.stop()
  }

}