package com.hongkuncc.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark35_RDD_Hbase {
  def main(args: Array[String]): Unit = {

    // TODO 1. 创建Spark配置对象
    val sparkConf = new SparkConf().setAppName("Spark35_RDD_Hbase").setMaster("local[*]")

    // TODO 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    val hbaseConf: Configuration = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "linux1,linux2,linux3")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "test")

    // TODO 3. 从Hbase中读取数据
    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
    val rdd2: RDD[String] = hbaseRDD.map {
      case (_, result) => Bytes.toString(result.getRow)
    }
    rdd2.collect.foreach(println)

    // TODO 9. 释放连接
    sc.stop()
  }

}
