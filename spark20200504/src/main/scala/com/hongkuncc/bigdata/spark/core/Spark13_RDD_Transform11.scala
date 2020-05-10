package com.hongkuncc.bigdata.spark.core


import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark13_RDD_Transform11 {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf对象
    val sparkConf = new SparkConf().setAppName("Spark07_RDD_Transform2").setMaster("local[*]")
    // 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    // 3. 构建RDD
    val rdd = sc.makeRDD(List(("a",1),("b",2),("c",3)),3)

    //使用分区器将数据重新分区
    //spark默认会提供HashPartitioner
    rdd partitionBy()

    //释放连接
    sc.stop()
  }

}

//自定义分区器
//1.继承Partitioner
class MyPartitioner extends Partitioner{
  override def numPartitions: Int = {
    partitions

  }

  override def getPartition(key: Any): Int = {

  }
}
