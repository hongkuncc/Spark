package com.hongkuncc.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.immutable

object Spark43_RDD_Req2_Top10CategoryTop10Session {

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

    // TODO 4. 将原始数据进行循环迭代，通过累加器进行聚合
    val accumulator = new MyCategoryCountAccumulator
    sc.register(accumulator)

    actionRDD.foreach(
      action=>{
        accumulator.add(action)
      }
    )

    // (鞋 - click ), 100)
    // (鞋 - order ), 100)
    // (鞋 - pay ), 100)
    val accumulatorValue: mutable.HashMap[(String, String), Long] = accumulator.value


    // 将累加器的结果根据品类ID进行分组
    val stringToTupleToLong: Map[String, mutable.HashMap[(String, String), Long]] = accumulatorValue.groupBy(kv=>kv._1._1)

    // 将分组后的数据转换为样例类对象，方便使用
    val infos: immutable.Iterable[CategoryCountInfo] = stringToTupleToLong.map {
      case (cid, map) => {
        CategoryCountInfo(cid, map.getOrElse((cid, "click"), 0L), map.getOrElse((cid, "order"), 0L), map.getOrElse((cid, "pay"), 0L))
      }
    }


    // TODO 5. 将聚合后的结果进行排序处理(降序)
    val top10CategoryInfos: List[CategoryCountInfo] = infos.toList.sortWith(
      (left, right) => {
        if (left.clickCount > right.clickCount) {
          true
        } else if (left.clickCount == right.clickCount) {
          if (left.orderCount > right.orderCount) {
            true
          } else if (left.orderCount == right.orderCount) {
            left.payCount > right.payCount
          } else {
            false
          }
        } else {
          false
        }
      }
    ).take(10)

    // TODO 将前10品类的对象转换结构（Info => Id）
    val ids: List[String] = top10CategoryInfos.map(info=>info.categoryId)
    //val value: Broadcast[List[String]] = sc.broadcast(ids)

    // ************************** 需求二 *****************************************
    // TODO 将原始数据进行筛选过滤，保留前10品类的数据
    val filterRDD: RDD[UserVisitAction] = actionRDD.filter(action => {
      if ( action.click_category_id != -1 ) {
        ids.contains(action.click_category_id.toString)
      } else {
        false
      }

      /*
      var flg = false
      breakable {
        top10CategoryInfos.foreach(
          info => {
            if (action.click_category_id.toString == info.categoryId) {
              flg = true
              break
            }
          }
        )
      }
      flg
       */
    })

    // TODO 将过滤后的数据进行结构的转换 UserVisitAction => (sessionId_categoryId, click)
    val sessionAndCategoryToOneRDD: RDD[(String, Long)] = filterRDD.map(action=>( action.session_id+"_"+action.click_category_id, 1L ))

    // TODO 将转换结构后的数据进行分组聚合
    val sessionAndCategoryToSumRDD: RDD[(String, Long)] = sessionAndCategoryToOneRDD.reduceByKey(_+_)

    // TODO 将聚合后的结果进行结构的转换(sessionId_categoryId, click) ==> (categoryId, (sessionId, clickSum))
    val categoryToSessionAndSumRDD: RDD[(String, (String, Long))] = sessionAndCategoryToSumRDD.map {
      case (k, sum) => {
        val ks: Array[String] = k.split("_")
        (ks(1), (ks(0), sum))
      }
    }

    // TODO 将转换结构的数据进行分组
    val groupRDD: RDD[(String, Iterable[(String, Long)])] = categoryToSessionAndSumRDD.groupByKey()

    // TODO 将分组后的数据进行排序（降序）取前10

    val result: RDD[(String, List[(String, Long)])] = groupRDD.mapValues(datas => {
      datas.toList.sortWith(
        (left, right) => {
          left._2 > right._2
        }
      ).take(10)
    })

    // 分布式排序
    //groupRDD.sortBy()

    // TODO 打印统计结果
    result.foreach(println)

    // ***************************************************************************

    // TODO 9. 释放连接
    sc.stop()
  }

}
