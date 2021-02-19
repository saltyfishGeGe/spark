package com.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_01_no_cache {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(List("Hello Spark", "Hello hive"))

    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRDD.map(k => {
      println("======= map ========")
      (k, 1)
    })

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)

    reduceRDD.collect().foreach(println)

    println("****************************************")

    /**
      * mapRDD: RDD可以复用，但是数据无法复用
      *         复用的RDD由于无缓存所以会重新读取一遍数据并处理
      */
    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

    groupRDD.collect().foreach(println)


    //======= map ========
    //======= map ========
    //======= map ========
    //======= map ========
    //(hive,1)
    //(Hello,2)
    //(Spark,1)
    //****************************************
    //======= map ========
    //======= map ========
    //======= map ========
    //======= map ========
    //(hive,CompactBuffer(1))
    //(Hello,CompactBuffer(1, 1))
    //(Spark,CompactBuffer(1))

    sc.stop()
  }

}
