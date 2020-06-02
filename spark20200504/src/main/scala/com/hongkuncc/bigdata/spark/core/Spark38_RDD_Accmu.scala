package com.hongkuncc.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark38_RDD_Accmu {
  def main(args: Array[String]): Unit = {

    // TODO 1. 创建Spark配置对象
    val sparkConf = new SparkConf().setAppName("Spark35_RDD_Hbase").setMaster("local[*]")

    // TODO 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(Int, Int)] = sc.makeRDD(List((1,1),(1,2),(1,3),(1,4)))
    //var sum = 0
    // 声明累加器
    val accumulator: LongAccumulator = sc.longAccumulator

    rdd.foreach{
      case ( k, v ) => {
        // 使用累加器
        accumulator.add(v)
      }
    }
    // 获取累加器的值
    println("sum = " + accumulator.value)

    // TODO 9. 释放连接
    sc.stop()
  }

}
