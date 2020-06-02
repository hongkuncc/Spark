package com.hongkuncc.bigdata.spark.core

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark40_RDD_Var {
  def main(args: Array[String]): Unit = {

    // TODO 1. 创建Spark配置对象
    val sparkConf = new SparkConf().setAppName("Spark40_RDD_Var").setMaster("local[*]")

    // TODO 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),4)

    val list = List(1,2,3,4)

    // 闭包所使用的变量是以Task为单位传递的
    // 所以如果希望减少数据的传输总量，可以以Executor为单位传递数据，那么就可以采用Spark中的广播变量

    // 声明广播变量
    val broadcast: Broadcast[List[Int]] = sc.broadcast(list)

    rdd.foreach( num=>{
      //println( num + list.sum )
      // 使用广播变量
      println(num + broadcast.value.sum)
    } )

    // TODO 9. 释放连接
    sc.stop()
  }

}
