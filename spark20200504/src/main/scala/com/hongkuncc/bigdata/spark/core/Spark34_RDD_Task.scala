package com.hongkuncc.bigdata.spark.core

import java.sql.{DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark34_RDD_Task {
  def main(args: Array[String]): Unit = {

    // TODO 1. 创建Spark配置对象
    val sparkConf = new SparkConf().setAppName("Spark08_RDD_Transform5").setMaster("local[*]")

    // TODO 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    // TODO 3. 向Mysql中写入数据
    val user = new User()
    val userListRDD: RDD[User] = sc.makeRDD(List[User](user))

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://linux1:3306/rdd"
    val userName = "root"
    val passWd = "000000"

    // 闭包检测
    // 检测闭包变量是否能序列化

    userListRDD.foreach{
      case u => {
        Class.forName(driver)
        val conn = DriverManager.getConnection(url, userName, passWd)
        val statement: PreparedStatement = conn.prepareStatement("insert into user (id, name, age) values (?, ?, ?)")
        statement.setInt(1, u.id)
        statement.setString(2, u.name)
        statement.setInt(3, u.age)
        statement.executeUpdate()
        statement.close()
        conn.close()
      }
    }


    // TODO 9. 释放连接
    sc.stop()
  }
}
class User {
  var id : Int = 1
  var name : String = "zhangsan"
  var age : Int = 30
}



