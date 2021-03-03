package com.bigdata.spark.sql.sql

import java.text.SimpleDateFormat
import java.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SaveMode, SparkSession}

object Spark07_sql_jdbc {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_mysql")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()


    //+--------------------+-----+-------------------+-------------------+--------------+---------+---+---+
    //|                 kid|isdel|         createTime|            delTime|gold_type_name|gold_type|num| id|
    //+--------------------+-----+-------------------+-------------------+--------------+---------+---+---+
    //|19122814-5832-481...| true|2019-12-28 14:58:31|2019-12-28 08:09:49|          充值|        0| 10|  1|
    //|19122815-0541-414...|false|2019-12-28 15:05:39|               null|       视频222|        1|  3|  2|
    //|20010413-1820-478...| true|2020-01-04 13:18:23|2020-01-04 05:46:24|       名称222|        1|  2|  3|
    //|20010413-2040-425...| true|2020-01-04 13:20:51|2020-01-04 05:28:36|       名称333|        2|  0|  4|
    //|20010413-2102-467...| true|2020-01-04 13:21:20|2020-01-04 05:39:12|       名称333|        2|  0|  5|
    //|20010413-4612-417...|false|2020-01-04 13:46:15|               null|      插图广告|        0|  4|  6|
    //|20010413-4753-487...|false|2020-01-04 13:47:57|               null|  邀请好友收益|        3| 20|  7|
    //+--------------------+-----+-------------------+-------------------+--------------+---------+---+---+

    val df: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/miniprogram")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "1234")
      .option("dbtable", "gold_icon")
      .load()

    val ds: Dataset[Gold] = df.as[Gold](Encoders.product)
    val result: Dataset[Gold] = ds.filter(line => {
      line.delTime == null
    })

    ds.createTempView("gold")

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    spark.udf.register("parseDate", (col: String) => {
      if(col != null) sdf.parse(col).getTime + "" else ""
    })

    val resultDF: DataFrame = spark.sql("select kid, isdel, parseDate(createTime) as createTime, parseDate(delTime) as delTime, gold_type_name, gold_type, num, id from gold")
    resultDF.show()

    // 写入一个新表
    resultDF.write.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/miniprogram")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "1234")
      .option("dbtable", "gold_icon_spark_test")
      .mode(SaveMode.Overwrite)
      .save()

    spark.close()
  }

  case class Gold(var kid:String,
                  var isdel:Boolean,
                  var createTime:String,
                  var delTime:String,
                  var gold_type_name:String,
                  var gold_type:Int,
                  var num:Int,
                  var id:Int
                 )

}
