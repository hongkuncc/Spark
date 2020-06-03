package com.hongkuncc.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.immutable

// 需求一 ： Top10 热门品类
// 第二种实现方式
object Spark42_RDD_Req1_1 {
  def main(args: Array[String]): Unit = {

    // TODO 1. 创建Spark配置对象
    val sparkConf = new SparkConf().setAppName("Spark41_RDD_Req1").setMaster("local[*]")

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
    val result: List[CategoryCountInfo] = infos.toList.sortWith(
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

    result.foreach(println)

    // TODO 9. 释放连接
    sc.stop()
  }
}

// 自定义累加器
class MyCategoryCountAccumulator extends AccumulatorV2[UserVisitAction, mutable.HashMap[(String, String), Long]]{

  private var map = new mutable.HashMap[(String, String), Long]()

  // 累加器是否初始化
  override def isZero: Boolean = {
    map.isEmpty
  }

  // 复制累加器
  override def copy(): AccumulatorV2[UserVisitAction, mutable.HashMap[(String, String), Long]] = {
    new MyCategoryCountAccumulator
  }

  // 重置累加器
  override def reset(): Unit = {
    map.clear()
  }

  // 增加数据
  override def add(v: UserVisitAction): Unit = {
    if ( v.click_category_id != -1 ) {
      // 点击数据的场合
      val key = ( v.click_category_id.toString, "click" )
      map(key) = map.getOrElse(key, 0L) + 1L
    } else if ( v.order_category_ids != "null" ) {
      // 下单的场合
      val cids: Array[String] = v.order_category_ids.split(",")
      cids.foreach(id=>{
        val key = ( id, "order" )
        map(key) = map.getOrElse(key, 0L) + 1L
      })
    } else if ( v.pay_category_ids != "null" ) {
      // 下单的场合
      val cids: Array[String] = v.pay_category_ids.split(",")
      cids.foreach(id=>{
        val key = ( id, "pay" )
        map(key) = map.getOrElse(key, 0L) + 1L
      })
    }
  }

  // 合并累加器
  override def merge(other: AccumulatorV2[UserVisitAction, mutable.HashMap[(String, String), Long]]): Unit = {
    val map1 = map
    val map2 = other.value

    map = map1.foldLeft(map2)(
      ( innerMap, kv ) => {
        innerMap(kv._1) = innerMap.getOrElse(kv._1, 0L) + kv._2
        innerMap
      }
    )
  }

  // 获取累加器的值
  override def value: mutable.HashMap[(String, String), Long] = {
    map
  }
}