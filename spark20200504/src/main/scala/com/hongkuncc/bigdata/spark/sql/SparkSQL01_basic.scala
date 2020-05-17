package com.hongkuncc.bigdata.spark.sql
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
object SparkSQL01_basic {
  def main(args: Array[String]): Unit = {


    // 创建一个新的 SparkSession 对象
    val sparkconf =  new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Basic")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    // 导入用到隐式转换.  如果想要使用: $"age" 则必须导入
    import spark.implicits._

    val df:DataFrame = spark.read.json( "input/user.json")
    // 打印信息
    //df.show()
    /*
        val users: Seq[User] = Seq( User(1,"zhangsan",30), User(2, "lisi", 40) )
        val ds: Dataset[User] = users.toDS
        ds.show
         */
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 30)))

    // RDD -> DF
    val df1: DataFrame = rdd.toDF("id", "name", "age")

    // DF -> RDD
    val rdd1: RDD[Row] = df1.rdd

    // RDD -> DS
    val userRDD: RDD[User] = rdd.map(t=>User(t._1, t._2, t._3))
    val ds1: Dataset[User] = userRDD.toDS()

    // DS -> RDD
    val rdd2: RDD[User] = ds1.rdd

    // DF -> DS
    val ds2: Dataset[User] = df1.as[User]
    // DS -> DF
    val df2: DataFrame = ds2.toDF()
    // 使用SQL语法访问数据
    //df.createOrReplaceTempView("user")

    //spark.sql("select * from user").show

    // 使用DSL语法访问数据
    //df.select("age").show()

    //val rdd: RDD[Row] = df.rdd
    //rdd.foreach(row=>row.get)


    //关闭连接
    spark.stop()
  }
}
case class User(id:Int, name:String, age:Int)
