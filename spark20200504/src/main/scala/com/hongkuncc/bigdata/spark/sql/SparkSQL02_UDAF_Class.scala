package com.hongkuncc.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

object SparkSQL02_UDAF_Class {
  def main(args: Array[String]): Unit = {
    // TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL02_UDAF")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    // TODO 读取数据
    val userDF: DataFrame = spark.read.json("input/user.json")
    val userDS: Dataset[Emp] = userDF.as[Emp]

    // 创建聚合函数
    val udaf = new AgeAvgClassUDAF

    // 将聚合函数转换为查询列
    val column: TypedColumn[Emp, Double] = udaf.toColumn

    // 使用DSL语法
    userDS.select(column).show


    // TODO 释放资源
    spark.stop()
  }

}
case class Emp( username:String, age:Long )
case class AvgBuffer( var total:Long, var count:Long )
// 用户自定义聚合函数(强类型) : 用户的平均年龄
// 1. 继承Aggregator, 增加泛型
// 2. 重写方法
class AgeAvgClassUDAF extends Aggregator[Emp, AvgBuffer, Double]{

  // 缓冲区初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0L, 0L)
  }

  // 聚合数据
  override def reduce(buffer: AvgBuffer, emp: Emp): AvgBuffer = {
    buffer.total = buffer.total + emp.age
    buffer.count = buffer.count + 1L
    buffer
  }

  // 合并多个聚合函数的结果
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.total = b1.total + b2.total
    b1.count = b1.count + b2.count
    b1
  }

  // 完成聚合函数（计算结果）
  override def finish(buff: AvgBuffer): Double = {
    buff.total.toDouble / buff.count
  }

  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}