package com.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_04_persist_diff {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")

    val sc = new SparkContext(sparkConf)

    /**
      * 1. cache: 临时存储在内存中进行数据重用，数据不安全，容易丢失，内存溢出
      *           会在血缘关系中添加新的依赖，一旦出现问题可以从头读取数据
      * 2. persist: 可临时存储在磁盘，磁盘IO较高，性能不如cache内存，数据较安全。数据为临时存储，作业执行完毕后将删除所有数据文件
      * 3. checkPoint: 数据长久保存在磁盘中，达到跨多个作业复用数据，数据安全，效率较低。
      *                为了保证数据安全，一般情况下会独立执行作业，即再一次处理数据
      *                为了提高效率，一般情况下与cache联合使用
      *                执行过程中会切断血缘关系，重新建立新的血缘，从checkPoint开始
      */

    sc.setCheckpointDir("checkPoint")

    val rdd: RDD[String] = sc.makeRDD(List("Hello Spark", "Hello hive"))

    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRDD.map(k => {
      println("======= map ========")
      (k, 1)
    })

    //mapRDD.cache()
    mapRDD.checkpoint()
    println(mapRDD.toDebugString)

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)

    reduceRDD.collect().foreach(println)

    println("****************************************")

    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

    groupRDD.collect().foreach(println)

    println(mapRDD.toDebugString)

    sc.stop()
  }

}
