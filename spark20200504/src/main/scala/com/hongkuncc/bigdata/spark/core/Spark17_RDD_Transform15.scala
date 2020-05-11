package com.hongkuncc.bigdata.spark.core


import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark17_RDD_Transform15 {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf对象
    val sparkConf = new SparkConf().setAppName("Spark07_RDD_Transform2").setMaster("local[*]")
    // 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    // 3. 构建RDD
    val rdd = sc.makeRDD(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)

    /*aggregateByKey
    参数描述:
      1.	zeroValue：给每一个分区中的每一个key一个初始值；
    2.	seqOp：函数用于在每一个分区中用初始值逐步迭代value；
    3.	combOp：函数用于合并每个分区中的结果。*/


    /*val aggregateByKeyRDD: RDD[(String, Int)] = rdd.aggregateByKey(10)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    )*/
    val aggregateByKeyRDD: RDD[(String, Int)] = rdd.aggregateByKey(10)(
      _+_,
      _+_
    )

    println(aggregateByKeyRDD.collect().mkString(","))

    //释放连接
    sc.stop()
  }

}