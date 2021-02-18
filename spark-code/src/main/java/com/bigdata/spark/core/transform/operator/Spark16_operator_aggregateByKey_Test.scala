package com.bigdata.spark.core.transform.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark16_operator_aggregateByKey_Test {

  def main(args: Array[String]): Unit = {

    val datas = List(("a", 3),("a", 2),("a", 3),("a", 4),("b", 4),("b", 4))

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(datas, 2)

    // 求平均值
    // (0, 0) 构造成一个Tuple，第一个值为总值，第二个值为次数
    // 手动构造第一个值(0, 0)，传入分区内进行聚合
    val aggRDD: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
      (t, v) => {
        println((t._1 + v, t._2 + 1))
        (t._1 + v, t._2 + 1)
      },
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )

    aggRDD.foreach(println)

    aggRDD.mapValues(num => {
      num._1 / num._2
    }).collect().foreach(println)

    //(3,1)
    //(4,1)
    //(5,2)
    //(4,1)
    //(8,3)
    //(8,2)
    //(a,(12,4))
    //(b,(8,2))
    //(b,4)
    //(a,3)
    sc.stop
  }

}