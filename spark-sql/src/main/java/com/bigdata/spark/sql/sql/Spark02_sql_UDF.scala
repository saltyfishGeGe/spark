package com.bigdata.spark.sql.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Spark02_sql_UDF {

  def main(args: Array[String]): Unit = {

    val conf : SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")

    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    /**
      * 使用DataFrame时，涉及转换操作，需要引入隐式转换规则，否则无法使用
      */
    import sparkSession.implicits._

    val df = sparkSession.read.json("datas/user.json")
    df.createOrReplaceTempView("user")

    // 在SQL中操作会被默认为列的字符串拼接，无法输出正确结果，需要使用UDF函数
    //+---+--------------------------------------------------+
    //|age|(CAST(Name: AS DOUBLE) + CAST(username AS DOUBLE))|
    //+---+--------------------------------------------------+
    //| 30|                                              null|
    //| 20|                                              null|
    //| 20|                                              null|
    //| 10|                                              null|
    //+---+--------------------------------------------------+
    // sparkSession.sql("select age, 'Name:' + username from user").show()

    // 注册UDF函数
    sparkSession.udf.register("prefixName", (name) => {
      "Name:" + name
    })

    sparkSession.sql("select age, prefixName(username) from user").show()

    sparkSession.close()
  }

}
