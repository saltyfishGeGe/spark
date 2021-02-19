package com.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_03_checkPoint {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")

    val sc = new SparkContext(sparkConf)

    // checkPoint 需要落盘，需要指定检查点保存路径，且作业运行完后不会删除
    // 一般保存路径为hadoop中
    sc.setCheckpointDir("checkPoint")

    val rdd: RDD[String] = sc.makeRDD(List("Hello Spark", "Hello hive"))

    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRDD.map(k => {
      println("======= map ========")
      (k, 1)
    })

    // 数据存入检查点
    mapRDD.checkpoint()

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)

    reduceRDD.collect().foreach(println)

    println("****************************************")

    // checkPoint复用数据
    // 为了保证数据安全，一般情况下会在一次执行作业
    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

    groupRDD.collect().foreach(println)

    // 此处map执行了两遍，由于checkPoint
    //======= map ========
    //======= map ========
    //======= map ========
    //======= map ========
    //======= map ========
    //======= map ========
    //======= map ========
    //======= map ========
    //(hive,1)
    //(Hello,2)
    //(Spark,1)
    //****************************************
    //(hive,CompactBuffer(1))
    //(Hello,CompactBuffer(1, 1))
    //(Spark,CompactBuffer(1))
    sc.stop()
  }

}
