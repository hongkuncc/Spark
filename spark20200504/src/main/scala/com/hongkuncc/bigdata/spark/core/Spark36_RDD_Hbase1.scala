package com.hongkuncc.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark36_RDD_Hbase1 {
  def main(args: Array[String]): Unit = {

    // TODO 1. 创建Spark配置对象
    val sparkConf = new SparkConf().setAppName("Spark35_RDD_Hbase").setMaster("local[*]")

    // TODO 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    val hbaseConf: Configuration = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "linux1,linux2,linux3")
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "test")

    // TODO 3. 向Hbase插入数据
    // Put
    val dataRDD: RDD[(String, String)] = sc.makeRDD(List( ("6001", "zhangsan"),("6002", "lisi"),("6003", "wnagwu") ))

    val job = Job.getInstance(hbaseConf)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Put])

    // data => put
    val hbaseRDD = dataRDD.map {
      case (rk, name) => {
        val put = new Put(Bytes.toBytes(rk))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name))
        (new ImmutableBytesWritable(Bytes.toBytes(rk)), put)
      }
    }

    hbaseRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)

    // TODO 9. 释放连接
    sc.stop()
  }

}
