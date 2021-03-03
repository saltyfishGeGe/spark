package com.bigdata.spark.sql.sql

import java.text.SimpleDateFormat

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types.StringType

object Spark07_sql_jdbc2 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_mysql")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val df: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/miniprogram")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "1234")
      .option("dbtable", "gold_icon")
      .load()

    df.printSchema()

    // createTime 和 delTime都是时间戳类型
    // 转换为字符类型
    val colms: Array[Column] = df.columns.map(c => {
      if (List("createTime", "delTime").contains(c)) {
        df(c).cast(StringType)
      } else {
        df(c)
      }
    })

    val df3: DataFrame = df.select(colms:_*)

    df3.printSchema()

    // 写入一个新表, 此时时间戳被转换为text格式存入
    df3.write.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/miniprogram")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "1234")
      .option("dbtable", "gold_icon_spark_test2")
      .mode(SaveMode.Append)
      .save()
    spark.close()
  }
}
