package com.hongkuncc.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

object Spark39_RDD_Accumu1 {
  def main(args: Array[String]): Unit = {

    // TODO 1. 创建Spark配置对象
    val sparkConf = new SparkConf().setAppName("Spark35_RDD_Hbase").setMaster("local[*]")

    // TODO 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, Long)] = sc.makeRDD(List(("xx",1L),("yy",2L),("xx",3L),("yy",4L)))

    // 创建累加器
    val accumu = new MyAccumulator
    // 注册累加器
    sc.register(accumu, "MyAccumulator")

    rdd.foreach{
      data=> {
        // 使用累加器
        accumu.add(data)
      }
    }

    // 返回累加器的值
    println(accumu.value)



    // TODO 9. 释放连接
    sc.stop()
  }
}
// 自定义累加器
// 1. 继承AccumulatorV2抽象类，并设定泛型In, Out
// 2. 重写方法
class MyAccumulator extends AccumulatorV2[(String, Long), java.lang.Long] {

  // xx -> 1, yy->2, aa->3
  private var sum = 0L

  // 累加器是否初始化
  override def isZero: Boolean = {
    sum == 0
  }

  //  复制新的累加器
  override def copy(): AccumulatorV2[(String, Long), lang.Long] = {
    new MyAccumulator
  }

  // 重置累加器
  override def reset(): Unit = {
    sum = 0L
  }

  // 累加计算
  override def add(t: (String, Long)): Unit = {
    if ( t._1 == "xx" ) {
      sum = sum + t._2
    }
  }

  // 合并累加器
  override def merge(other: AccumulatorV2[(String, Long), lang.Long]): Unit = {
    sum = sum + other.value
  }

  // 累加器的值
  override def value: lang.Long = {
    sum
  }

}
