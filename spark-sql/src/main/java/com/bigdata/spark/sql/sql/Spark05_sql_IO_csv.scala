package com.bigdata.spark.sql.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Spark05_sql_IO_csv {

  def main(args: Array[String]): Unit = {

    val conf : SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")

    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = sparkSession.read.format("csv")
      .option("sep", ";")
      .option("header", true)
      .option("inferSchema", true)
      .load("datas/people.csv")

    df.show()

    sparkSession.close()
  }
}
