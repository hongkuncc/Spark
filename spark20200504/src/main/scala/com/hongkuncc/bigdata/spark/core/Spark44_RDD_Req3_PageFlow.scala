package com.hongkuncc.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 需求三 ： 页面单跳转换率
object Spark44_RDD_Req3_PageFlow {
  def main(args: Array[String]): Unit = {

    // TODO 1. 创建Spark配置对象
    val sparkConf = new SparkConf().setAppName("Spark43_RDD_Req2_Top10CategoryTop10Session").setMaster("local[*]")

    // TODO 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    // TODO 3. 获取原始数据, 转换为样例类对象
    val dataRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")
    val actionRDD: RDD[UserVisitAction] = dataRDD.map(line => {
      val datas: Array[String] = line.split("_")
      UserVisitAction(
        datas(0),
        datas(1).toLong,
        datas(2),
        datas(3).toLong,
        datas(4),
        datas(5),
        datas(6).toLong,
        datas(7).toLong,
        datas(8),
        datas(9),
        datas(10),
        datas(11),
        datas(12).toLong
      )
    })

    // TODO 需要统计页面单跳转换率的页面路径
    val pageflows = List(1,2,3,4,5,6)
    // List(  (1-2),(2-3),(3-4) )
    val pageflowZip: List[(Int, Int)] = pageflows.zip(pageflows.tail)
    val pageflowStringList: List[String] = pageflowZip.map(t=>t._1 + "-"+t._2)

    // 分母
    // 将数据转换成结构 （ pageid, 1L）
    val filterRDD: RDD[UserVisitAction] = actionRDD.filter(action=>pageflows.contains(action.page_id.toInt))
    val pageIdToOneRDD: RDD[(Long, Long)] = filterRDD.map(action=>{ (action.page_id,1L ) })
    val pageIDToSumMap: collection.Map[Long, Long] = pageIdToOneRDD.countByKey()

    // 分子
    // TODO 将原始数据根据Sessionid进行分组
    val sessionGroupRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(action=>action.session_id)

    // TODO 将分组后的数据根据页面访问时间进行排序（升序）
    val sessionToPageFlows: RDD[(String, List[(String, Long)])] =
      sessionGroupRDD.mapValues(datas => {
        val actions: List[UserVisitAction] = datas.toList.sortWith(
          (left, right) => {
            left.action_time < right.action_time
          }
        )
        // [1,2,3,4,5,6,7]
        // [2,3,4,5,6,7]
        val pageIds: List[Long] = actions.map(_.page_id)

        // (1,2),(2,3),(20,3)
        val pageIdToPageIds: List[(Long, Long)] = pageIds.zip(pageIds.tail)

        // (1,2),(2,3)
        val filterList: List[(Long, Long)] = pageIdToPageIds.filter {
          case (pid1, pid2) => {
            pageflowStringList.contains(pid1 + "-" + pid2)
          }
        }
        /*
        val iterator: Iterator[List[Long]] = pageIds.sliding(2)
        while ( iterator.hasNext ) {
          val pageidTwos: List[Long] = iterator.next()
          ( pageidTwos(0) + "-" + pageidTwos(1),1 )
        }
         */
        // (1-2,1),(2-3,1),(3-4,1),(4-5,1),(5-6,1),(6-7,1)
        val pageFlowToOne: List[(String, Long)] = filterList.map {
          case (pid1, pid2) => {
            (pid1 + "-" + pid2, 1L)
          }
        }

        pageFlowToOne
      })
    // 将拉链后的数据进行结构的转换（session, Iterator((pageid1-pageid2, 1))）=> Iterator((pageid1-pageid2, 1))
    // List( (1-2,1), (2-3,1), (1-2,1) )
    val pageflowsRDD: RDD[List[(String, Long)]] = sessionToPageFlows.map {
      case (sessionId, datas) => {
        datas
      }
    }
    // 将转换结构后的数据进行扁平化操作
    // (1-2,1), (2-3,1), (1-2,1)
    val pageflowRDD: RDD[(String, Long)] = pageflowsRDD.flatMap(datas=>datas)

    // 将扁平化后的数据进行分组聚合
    // (1-2,100), (2-3,50)
    val pageflowToCountRDD: collection.Map[String, Long] = pageflowRDD.countByKey()

    // 分子 / 分母
    pageflowToCountRDD.foreach{
      case ( pageflow, value1 )=> {
        val pids = pageflow.split("-")
        val pid1 = pids(0)
        val value2: Long = pageIDToSumMap.getOrElse(pid1.toLong, 1L)
        println( pageflow + "=" + ( value1.toDouble / value2 ) )
      }
    }

    // TODO 9. 释放连接
    sc.stop()
  }
}
