package com.hongkuncc.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

// 需求一 ： Top10 热门品类
object Spark41_RDD_Req1 {
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
    // TODO 4.
    //  4.1 过滤不需要数据,
    //  4.2. 将下单和支付的数据进行扁平化操作 id1,id2,id3 ==> (id1,1), (id2,1),(id3,1)
    // CategoryCountInfo(鞋，1，0，0)
    // CategoryCountInfo(衣服，0，1，0)
    val flatMapRDD: RDD[CategoryCountInfo] = actionRDD.flatMap(action => {
      action match {
        case UserVisitAction(
        date: String,
        user_id: Long,
        session_id: String,
        page_id: Long,
        action_time: String,
        search_keyword: String,
        click_category_id: Long,
        click_product_id: Long,
        order_category_ids: String,
        order_product_ids: String,
        pay_category_ids: String,
        pay_product_ids: String,
        city_id: Long
        ) if (click_category_id != -1) => List(CategoryCountInfo(click_category_id.toString, 1, 0, 0))
        case UserVisitAction(
        date: String,
        user_id: Long,
        session_id: String,
        page_id: Long,
        action_time: String,
        search_keyword: String,
        click_category_id: Long,
        click_product_id: Long,
        order_category_ids: String,
        order_product_ids: String,
        pay_category_ids: String,
        pay_product_ids: String,
        city_id: Long
        ) if (order_category_ids != "null") => {
          val cids: mutable.ArrayOps[String] = order_category_ids.split(",")
          val list = new ListBuffer[CategoryCountInfo]()
          cids.foreach(
            id => {
              list.append(CategoryCountInfo(id, 0, 1, 0))
            }
          )
          list
        }
        case UserVisitAction(
        date: String,
        user_id: Long,
        session_id: String,
        page_id: Long,
        action_time: String,
        search_keyword: String,
        click_category_id: Long,
        click_product_id: Long,
        order_category_ids: String,
        order_product_ids: String,
        pay_category_ids: String,
        pay_product_ids: String,
        city_id: Long
        ) if (pay_category_ids != "null") => {
          val cids: mutable.ArrayOps[String] = pay_category_ids.split(",")
          val list = new ListBuffer[CategoryCountInfo]()
          cids.foreach(
            id => {
              list.append(CategoryCountInfo(id, 0, 0, 1))
            }
          )
          list
        }
        case _ => Nil
      }
    })
    // TODO 5. 将相同的品类数据进行分组
    val groupRDD: RDD[(String, Iterable[CategoryCountInfo])] = flatMapRDD.groupBy(info=>info.categoryId)

    // TODO 6. 将分组后的数据进行聚合
    val infoRDD: RDD[CategoryCountInfo] = groupRDD.map {
      case (cid, datas) => {
        datas.reduce((x, y) => {
          x.clickCount = x.clickCount + y.clickCount
          x.orderCount = x.orderCount + y.orderCount
          x.payCount = x.payCount + y.payCount
          x
        })
      }
    }

    // TODO 7. 将数据进行排序（降序）（点击，下单，支付）
    val result: Array[CategoryCountInfo] = infoRDD.sortBy(info=>( info.clickCount, info.orderCount, info.payCount ), false).take(10)

    // TODO 8. 将结果打印到控制台
    result.foreach(println)

    // TODO 9. 释放连接
    sc.stop()
  }
}
/**
  * 用户访问动作表
  *
  * @param date               用户点击行为的日期
  * @param user_id            用户的ID
  * @param session_id         Session的ID
  * @param page_id            某个页面的ID
  * @param action_time        动作的时间点
  * @param search_keyword     用户搜索的关键词
  * @param click_category_id  某一个商品品类的ID
  * @param click_product_id   某一个商品的ID
  * @param order_category_ids 一次订单中所有品类的ID集合
  * @param order_product_ids  一次订单中所有商品的ID集合
  * @param pay_category_ids   一次支付中所有品类的ID集合
  * @param pay_product_ids    一次支付中所有商品的ID集合
  * @param city_id            城市 id
  */
case class UserVisitAction(date: String,
                           user_id: Long,
                           session_id: String,
                           page_id: Long,
                           action_time: String,
                           search_keyword: String,
                           click_category_id: Long,
                           click_product_id: Long,
                           order_category_ids: String,
                           order_product_ids: String,
                           pay_category_ids: String,
                           pay_product_ids: String,
                           city_id: Long)

case class CategoryCountInfo(categoryId: String,
                             var clickCount: Long,
                             var orderCount: Long,
                             var payCount: Long)
