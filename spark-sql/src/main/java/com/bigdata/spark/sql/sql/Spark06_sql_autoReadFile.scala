package com.bigdata.spark.sql.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Spark06_sql_autoReadFile {

  def main(args: Array[String]): Unit = {

    val conf : SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")

    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 通过``直接引入文件，减少手动读取文件步骤
    sparkSession.sql("select * from json.`datas/user.json`").show()

    sparkSession.sql("select * from parquet.`datas/users.parquet`").show()

    sparkSession.sql("select * from text.`datas/data4.txt`").show()

    sparkSession.close()
  }
}
