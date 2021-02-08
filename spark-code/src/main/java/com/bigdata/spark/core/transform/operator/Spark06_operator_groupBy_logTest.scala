package com.bigdata.spark.core.transform.operator

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_operator_groupBy_logTest {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.textFile("datas/apache.log")

    // TODO 对apache.log中的时间进行分组

    // 第一种写法
//    val groupByRDD: RDD[(String, Iterable[String])] = rdd.groupBy(line => {
//      line.split(" ")(3).substring(0, 13)
//    })


    // 第二种写法
    val groupByRDD: RDD[(Int, Int)] = rdd.map(line => {
      val splits: Array[String] = line.split(" ")
      val time = splits(3)
      val sdf = new SimpleDateFormat("dd/MM/yyyy:hh:mm:ss")
      val date: Date = sdf.parse(time)
      val hours: Int = date.getHours
      (hours, 1)
    }).groupBy(_._1).map(re => (re._1, re._2.size))

    groupByRDD.foreach(println)

    groupByRDD.collect()

    sc.stop
  }

}
