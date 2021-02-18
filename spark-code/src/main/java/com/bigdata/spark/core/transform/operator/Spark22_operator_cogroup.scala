package com.bigdata.spark.core.transform.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark22_operator_cogroup {

  def main(args: Array[String]): Unit = {

    val datas1 = List(("a", 2),("b", 3),("b", 4),("c", 6))
    val datas2 = List(("a", 1),("b", 5),("b", 6))

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(datas1, 2)
    val rdd2: RDD[(String, Int)] = sc.makeRDD(datas2, 2)

    // 分组连接
    // 同一个组中的数据会先连接在一起，若另外的rdd中不存在值则会返回空
    val cgRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)

    //(b,(CompactBuffer(3, 4),CompactBuffer(5, 6)))
    //(a,(CompactBuffer(2),CompactBuffer(1)))
    //(c,(CompactBuffer(6),CompactBuffer()))
    cgRDD.collect().foreach(println)

    sc.stop
  }

}