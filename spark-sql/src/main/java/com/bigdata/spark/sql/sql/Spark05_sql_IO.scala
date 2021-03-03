package com.bigdata.spark.sql.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

object Spark05_sql_IO {

  def main(args: Array[String]): Unit = {

    val conf : SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")

    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 1. 读取json文件
    val df = sparkSession.read.json("datas/user.json")
    df.show()

    // 2. 读取parquet文件, sparkSQL默认读取和写的是parquet文件类型
    val df2: DataFrame = sparkSession.read.load("datas/users.parquet")
    df2.show()

    // df2.write.save("output")

    // 3. 修改sparkSQL读取文件格式, 此方式 = read.json
    val df3: DataFrame = sparkSession.read.format("json").load("datas/user.json")
    df3.show()

    // 4. 保存其他文件格式的文件, 此方式 = write.json
    df3.write.format("json").save("output")

    // TODO 默认情况下，如果输出目录存在会导致write异常，若想在同一个目录下追加文件需调整保存模式
    //  SaveMode.Overwrite ErrorIfExists Ignore Append
    df2.write.mode(SaveMode.Append).save("output")

    sparkSession.close()
  }
}
