package com.hongkuncc.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL03_JDBC {
  def main(args: Array[String]): Unit = {
    // TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL03_JDBC")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    // TODO 读取数据
    val df: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://hduser102:3306/test")
      .option("user", "root")
      .option("password", "123123")
      .option("dbtable", "test")
      .load()

    // TODO 写入数据
    df.write.mode("append").format("jdbc")
      .option("url", "jdbc:mysql://hduser102:3306/test")
      .option("user", "root")
      .option("password", "123123")
      .option("dbtable", "test1")
      .save()

    //spark.read.jdbc()


    // TODO 释放资源
    spark.stop()
  }

}
