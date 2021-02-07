package com.bigdata.spark.core.operator

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark15_operator_groupByKey {

  def main(args: Array[String]): Unit = {

    val datas = List(("a", 1),("a", 2),("a", 3),("b", 1),("b", 2),("c", 1),("d", 1))

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(datas, 2)

    // groupByKey: 得到一个key: iter[values]的集合,集合中只包含value值，key是固定的
    // groupBy：得到一个key:iter[元组]的集合，集合是一个完整的数据元组， key可动态设置
    val groupByKeyRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey(1)

    /**
      * groupByKey:会导致数据打乱，存在shuffle操作
      * 在spark中，shuffle操作必须落盘处理，不能在内存中数据等待，会导致内存溢出，shuffle操作性能低
      *
      * groupByKey与reduceByKey对比：
      *           ①reduceByKey在shuffle前先进行部分汇聚，shuffle数据量下降，性能较高(combine)
      *           ②功能区别：一个是数据分组，一个是数据聚合
      *           ③reduceByKey的shuffle操作性能优于groupByKey
      * 相同点：①都存在shuffle操作  ②reduceByKey在读取分组数据后会在内存中进行聚合
      */

    //key=d, values=1
    //key=a, values=1,2,3
    //key=b, values=1,2
    //key=c, values=1
    groupByKeyRDD.foreach(t => { println(s"key=${t._1}, values=${t._2.mkString(",")}")})

    groupByKeyRDD.collect()

    sc.stop
  }

}