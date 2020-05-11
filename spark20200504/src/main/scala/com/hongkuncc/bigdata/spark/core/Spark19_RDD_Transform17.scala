package com.hongkuncc.bigdata.spark.core


import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark19_RDD_Transform17 {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf对象
    val sparkConf = new SparkConf().setAppName("Spark07_RDD_Transform2").setMaster("local[*]")
    // 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    // 3. 构建RDD
    val rdd = sc.makeRDD(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)

    //combineByKey
    val combineByKeyRDD: RDD[(String, (Int,Int))] = rdd.combineByKey(
      num=>(num,1),
      (t:(Int,Int),num) =>(t._1 +num,t._2+1),
      (t1:(Int,Int),t2:(Int,Int))=> (t1._1+t2._1,t1._2+t2._2)
    )
    val combineRDD: RDD[(String, Int)] = combineByKeyRDD.map {
      case (key, (sum, size)) => {
        (key, sum / size)
      }
    }


    println(combineRDD.collect().mkString(","))

    //释放连接
    sc.stop()
  }

}