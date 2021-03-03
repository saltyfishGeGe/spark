package com.bigdata.spark.sql.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types.StringType

object Spark08_sql_hive {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_mysql")

    val spark: SparkSession = SparkSession.builder()
      .config(sparkConf)
      .config("spark.sql.warehouse.dir", "hdfs://xianyu01:8020/hive") // 虚拟机hadoop集群的仓库目录
      .enableHiveSupport() // 开启hive支持
      .getOrCreate()

    // 若有权限问题，可以调整为对应目录所属的用户
    // System.setProperty("HADOOP_USER_NAME", "root")

    spark.sql("show databases").show()

    spark.sql("show tables").show()

    spark.sql("select * from cur.scan_log limit 30").show()

    spark.close()
  }
}
