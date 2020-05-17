package com.hongkuncc.bigdata.spark.sql
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
object SparkSQL01_basic {
  def main(args: Array[String]): Unit = {


    // 创建一个新的 SparkSession 对象
    val sparkconf = new SparkConf().setAppName("hello world").setMaster("local[*]")
    val sc = new SparkContext(sparkconf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // 导入用到隐式转换.  如果想要使用: $"age" 则必须导入
    import sqlContext.implicits._

    val df = sqlContext.read.json("file://" + ClassLoader.getSystemResource("user.json").getPath)
    // 打印信息
    df.show
    // 查找年龄大于19岁的
    df.filter($"age" > 19).show

    // 创建临时表
    df.createTempView("user")
    sqlContext.sql("select * from user where age > 19").show

    //关闭连接
    sc.stop()

  }

}
