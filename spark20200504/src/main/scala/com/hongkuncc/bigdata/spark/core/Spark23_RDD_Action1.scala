package com.hongkuncc.bigdata.spark.core


import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import  org.apache.spark.rdd.RDD

object Spark23_RDD_Action1 {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf对象
    val sparkConf = new SparkConf().setAppName("Spark07_RDD_Transform2").setMaster("local[*]")
    // 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    // 3. 构建RDD
    val rdd1 = sc.parallelize(1 to 100)
    //reduce
/*    val reduceRDD: Int = rdd1.reduce(_ + _)
    println(reduceRDD)*/
    rdd1.collect().foreach(println)
//    println(rdd1.count())
    rdd1.take(3).foreach(println)
    println(rdd1.first())//=take(1)

   /* val rdd2 = sc.parallelize(Array(("a", 1), ("b", 2), ("c", 3)))
    val reduceRDD: (String, Int) = rdd2.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    println(reduceRDD)*/

    val rdd3 = sc.makeRDD(Array(100, 20, 130, 500, 60))
    rdd3.takeOrdered(2).foreach(println)
    rdd3.takeOrdered(2)(Ordering.Int.reverse).foreach(println)

    //释放连接
    sc.stop()
  }

}