package com.bigdata.spark.sql.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Spark09_sql_hive_test_top3 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_mysql")

    val spark: SparkSession = SparkSession.builder()
      .config(sparkConf)
      .config("spark.sql.warehouse.dir", "hdfs://xianyu01:8020/hive") // 虚拟机hadoop集群的仓库目录
      .enableHiveSupport() // 开启hive支持
      .getOrCreate()

    // 若有权限问题，可以调整为对应目录所属的用户
     System.setProperty("HADOOP_USER_NAME", "root")

    // TODO 从点击量，计算出各个区域前三大热门商品

    // 由于查询比较慢，先用个临时表存下中间结果
    spark.sql(
      """
        |CREATE TABLE bigdata.tmp_topThree as
        |SELECT
        |c.area area,
        |c.city_id city_id,
        |p.product_id product_id,
        |COUNT(click_product_id) clickNum
        |FROM user_visit_action a
        |INNER JOIN city_info c ON c.city_id = a.city_id
        |INNER JOIN product_info p ON p.product_id = a.click_product_id
        |GROUP BY
        |c.area,
        |c.city_id,
        |p.product_id
      """.stripMargin)

    spark.sql(
      """
        |SELECT b.*, c.city_name, p.product_name
        |FROM
        |(
        |  SELECT a.*,
        |  row_number() over(partition by a.area order by a.totalClick desc) as rn
        |  FROM
        |  (SELECT
        |    area,
        |    product_id,
        |    city_id,
        |    sum(clickNum) totalClick
        |    FROM
        |    tmp_topThree
        |    GROUP BY
        |    area,
        |    product_id,
        |    city_id
        |    )a
        |) b
        |INNER JOIN city_info c ON c.city_id = b.city_id
        |INNER JOIN product_info p ON p.product_id = b.product_id
        |where b.rn < 4
      """.stripMargin)

    // 区域，产品ID, 地市ID，点击总数，地市名称，产品名称
    // 华南	5	3	73	1	深圳	商品_5
    // 华南	16	20	67	2	福州	商品_16
    // 华南	85	20	66	3	福州	商品_85

    spark.close()
  }
}
