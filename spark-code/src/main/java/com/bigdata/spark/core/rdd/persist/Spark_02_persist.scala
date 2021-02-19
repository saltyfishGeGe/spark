package com.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_02_persist {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(List("Hello Spark", "Hello hive"))

    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRDD.map(k => {
      println("======= map ========")
      (k, 1)
    })

    // 缓存中间数据
    // map中的数据只会处理一次
    // cache方法默认调用persist方法，MEMORY_ONLY，只缓存在内存中，persist方法可手动设置存储级别
    // DISK_ONLY_2 : 2表示副本数，可选内存和磁盘并存，内存满则溢写
    // 持久化将在行动算子执行时触发！！
    mapRDD.cache()
    // mapRDD.persist()

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)

    reduceRDD.collect().foreach(println)

    println("****************************************")

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
    //(hive,CompactBuffer(1))
    //(Hello,CompactBuffer(1, 1))
    //(Spark,CompactBuffer(1))

    /**
      * RDD 持久化并不是一定为了数据复用，在执行时间较长的操作后，可以对重要数据进行保存
      */
    sc.stop()
  }

}
