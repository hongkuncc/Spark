package com.hongkuncc.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark22_RDD_Transform_Demo {
  /*
  数据结构：时间戳，省份，城市，用户，广告 字段使用空格分割。

  样本如下：
  1516609143867 6 7 64 16
  1516609143869 9 4 75 18
  1516609143869 1 7 87 12

  统计出每一个省份广告被点击次数的 TOP3
   */
    def main(args: Array[String]): Unit = {
      // 1. 初始化spark配置信息, 并建立到spark的连接
      val conf = new SparkConf().setAppName("Practice").setMaster("local[*]")
      val sc = new SparkContext(conf)
      // 2. 从文件中读取数据, 得到 RDD. RDD中存储的是文件的中的每行数据
      val lines: RDD[String] = sc.textFile("C:\\Users\\hongk\\Desktop\\Spark\\input\\agent.log")

      // 3. ((provice, ad), 1)
      val provinceADAndOne: RDD[((String, String), Int)] = lines.map(line => {
        val splits: Array[String] = line.split(" ")
        ((splits(1), splits(4)), 1)
      })
      // 4. 计算每个省份每个广告被点击的总次数
      val provinceADSum: RDD[((String, String), Int)] = provinceADAndOne.reduceByKey(_ + _)

      // 5. 将省份作为key，广告加点击数为value： (Province,(AD,sum))
      val provinceToAdSum: RDD[(String, (String, Int))] = provinceADSum.map(x => (x._1._1, (x._1._2, x._2)))

      // 6. 按照省份进行分组
      val provinceGroup: RDD[(String, Iterable[(String, Int)])] = provinceToAdSum.groupByKey()

      //7. 对同一个省份的广告进行排序, 按照点击数的降序

      val result: RDD[(String, List[(String, Int)])] = provinceGroup.mapValues {
          x => x.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
        }

      //8. 按照省份的升序展示最终结果
      val result2: RDD[(String, List[(String, Int)])] = provinceGroup.mapValues{
        x =>x.toList.sortWith((x,y)=>x._2>y._2).take(3)
      }

      // 9. 关闭连接
      result.sortBy(_._1).collect.foreach(println)

      sc.stop()



  }

}
