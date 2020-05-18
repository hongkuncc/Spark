package com.hongkuncc.bigdata.spark.sql

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf}

object SparkSQL02_UDAF {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wc")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    // TODO 读取数据
    val userDF: DataFrame = spark.read.json("input/user.json")
    userDF.createOrReplaceTempView("user")
    spark.sql("select avg(age) from user").show
    // 创建聚合函数
    val udaf = new AgeAvgUDAF
    // 向spark注册
    spark.udf.register("ageAvg", udaf)

    spark.sql("select ageAvg(age) from user").show

    // TODO 释放资源
    spark.stop()

  }
}
// 用户自定义聚合函数 : 用户的平均年龄
class AgeAvgUDAF extends UserDefinedAggregateFunction{

  // 表示聚合函数的输入的数据结构
  override def inputSchema: StructType = {
    StructType(Array(StructField("age", LongType)))
  }

  // 表示聚合运算时缓冲区的数据结构
  override def bufferSchema: StructType = {
    StructType(Array(StructField("total", LongType), StructField("count", LongType)))
  }

  // 聚合函数的结果类型
  override def dataType: DataType = {
    DoubleType
  }

  // 稳定性：传递一个值到聚合函数中，返回结果是否相同（幂等性）
  override def deterministic: Boolean = true

  // 聚合函数的初始化(缓冲区初始化)
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  // 聚合函数在聚合数据时对缓冲区的更新
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1L
  }

  // 多个聚合函数的缓冲区的合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // 计算聚合函数的结果
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)


  }

}
