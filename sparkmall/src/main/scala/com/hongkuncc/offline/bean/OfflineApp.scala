package com.hongkuncc.offline.bean

import java.util.UUID

import com.hongkuncc.common.bean.UserVisitAction
import com.hongkuncc.common.util.ConfigurationUtil
import org.apache.spark.sql.SparkSession

object OfflineApp {
  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("OfflineApp")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "hdfs://hadoop201:9000/user/hive/warehouse")
      .getOrCreate()
    val taskId = UUID.randomUUID().toString
    // 根据条件过滤取出需要的 RDD, 过滤条件定义在配置文件中
    val userVisitActionRDD = readUserVisitActionRDD(spark, readConditions)


    println("任务1: 开始")
    CategoryTop10App.statCategoryTop10(spark, userVisitActionRDD, taskId)
    println("任务1: 结束")

  }

  /**
    * 读取指定条件的 UserVisitActionRDD
    *
    * @param spark
    * @param condition
    */
  def readUserVisitActionRDD(spark: SparkSession, condition: Condition) = {
    var sql = s"select v.* from user_visit_action v join user_info u on v.user_id=u.user_id where 1=1"
    if (isNotEmpty(condition.startDate)) {
      sql += s" and v.date>='${condition.startDate}'"
    }
    if (isNotEmpty(condition.endDate)) {
      sql += s" and v.date<='${condition.endDate}'"
    }

    if (condition.startAge != 0) {
      sql += s" and u.age>=${condition.startAge}"
    }
    if (condition.endAge != 0) {
      sql += s" and u.age<=${condition.endAge}"
    }
    spark.sql("use sparkmall")
    spark.sql(sql).as[UserVisitAction].rdd
  }

  /**
    * 读取过滤条件
    *
    * @return
    */
  def readConditions: Condition = {
    // 读取配置文件
    val config = ConfigurationUtil("conditions.properties")
    // 读取到其中的 JSON 字符串
    val conditionString = config.getString("condition.params.json")
    // 解析成 Condition 对象
    JSON.parseObject(conditionString, classOf[Condition])

  }


}
