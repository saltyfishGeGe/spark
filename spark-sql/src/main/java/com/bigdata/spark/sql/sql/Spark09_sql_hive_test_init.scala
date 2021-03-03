package com.bigdata.spark.sql.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Spark09_sql_hive_test_init {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_mysql")

    val spark: SparkSession = SparkSession.builder()
      .config(sparkConf)
      .config("spark.sql.warehouse.dir", "hdfs://xianyu01:8020/hive") // 虚拟机hadoop集群的仓库目录
      .enableHiveSupport() // 开启hive支持
      .getOrCreate()

    // 若有权限问题，可以调整为对应目录所属的用户
     System.setProperty("HADOOP_USER_NAME", "root")


    // 导入本地文件并建表
    spark.sql(
      """
        |CREATE TABLE if not exists bigdata.user_visit_action(
        | `date` string,
        | `user_id` bigint,
        | `session_id` string,
        | `page_id` bigint,
        | `action_time` string,
        | `search_keyword` string,
        | `click_category_id` bigint,
        | `click_product_id` bigint,
        | `order_category_ids` string,
        | `order_product_ids` string,city_info
        | `pay_category_ids` string,
        | `pay_product_ids` string,
        | `city_id` bigint)
        |row format delimited fields terminated by '\t'
      """.stripMargin)

    spark.sql(
      """
        |load data local inpath 'datas/sql/user_visit_action.txt' into table bigdata.user_visit_action
      """.stripMargin)

    spark.sql(
      """
        |CREATE TABLE if not exists bigdata.product_info(
        | `product_id` bigint,
        | `product_name` string,
        | `extend_info` string)
        |row format delimited fields terminated by '\t'
      """.stripMargin)

    spark.sql(
      """
        |load data local inpath 'datas/sql/product_info.txt' into table bigdata.product_info
      """.stripMargin)

    spark.sql(
      """
        |CREATE TABLE if not exists bigdata.city_info(
        | `city_id` bigint,
        | `city_name` string,
        | `area` string)
        |row format delimited fields terminated by '\t'
      """.stripMargin)

    spark.sql(
      """
        |load data local inpath 'datas/sql/city_info.txt' into table bigdata.city_info
      """.stripMargin)

    spark.close()
  }
}
