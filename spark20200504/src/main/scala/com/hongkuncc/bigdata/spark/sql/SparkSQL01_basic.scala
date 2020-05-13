package com.hongkuncc.bigdata.spark.sql
import
object SparkSQL01_basic {
  def main(args: Array[String]): Unit = {


    // 创建一个新的 SparkSession 对象
    val spark: SparkSession = new SparkSession.builder()
      .master("local[*]")
      .appName("Word Count")
      .getOrCreate()
    // 导入用到隐式转换.  如果想要使用: $"age" 则必须导入
    val df = spark.read.json("file://" + ClassLoader.getSystemResource("user.json").getPath)
    // 打印信息
    df.show
    // 查找年龄大于19岁的
    df.filter($"age" > 19).show

    // 创建临时表
    df.createTempView("user")
    spark.sql("select * from user where age > 19").show

    //关闭连接
    spark.stop()

  }

}
