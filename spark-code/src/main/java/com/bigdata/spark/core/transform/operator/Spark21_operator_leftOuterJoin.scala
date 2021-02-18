package com.bigdata.spark.core.transform.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark21_operator_leftOuterJoin {

  def main(args: Array[String]): Unit = {

    val datas1 = List(("a", 2),("b", 3),("b", 4),("c", 6))
    val datas2 = List(("a", 1),("b", 5),("b", 6))

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(datas1, 2)
    val rdd2: RDD[(String, Int)] = sc.makeRDD(datas2, 2)

    val leftJoinRDD: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)

    //(b,(3,Some(5)))
    //(b,(3,Some(6)))
    //(b,(4,Some(5)))
    //(b,(4,Some(6)))
    //(a,(2,Some(1)))
    //(c,(6,None))
    // 右连接的集合位置与左连接相反
    leftJoinRDD.collect().foreach(println)

    sc.stop
  }

}