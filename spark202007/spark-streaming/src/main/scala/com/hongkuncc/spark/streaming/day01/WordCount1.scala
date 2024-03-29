package com.hongkuncc.spark.streaming.day01


import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/3/23 9:30
 */
object WordCount1 {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "hongkuncc")
        // 1. 创建StreamingContext
        val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount1")
        val ssc = new StreamingContext(conf, Seconds(3))
        // 2. 从数据源创建一个流:  socket, rdd队列, 自定义接收器,    kafka(重点)
        val sourceStream = ssc.socketTextStream("hduser102", 9999)
        // 3. 对流做各种转换
        val result = sourceStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
        //        val result = sourceStream.flatMap(x => {Thread.sleep(10000); x.split(" ")}).map((_, 1)).reduceByKey(_ + _)
        // 4. 行动算子 print  foreach foreachRDD
//        result.print() // 把结果打印在控制台
        result.saveAsNewAPIHadoopFiles("/1015/test",
            "wc",
            classOf[Text],
            classOf[IntWritable],
            classOf[TextOutputFormat[Text, IntWritable]])
        // 5. 启动流
        ssc.start()
        // 6. 阻止主线程退出(阻塞主线程)
        ssc.awaitTermination()
    }
}
