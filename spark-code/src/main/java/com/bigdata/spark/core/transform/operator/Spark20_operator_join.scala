package com.bigdata.spark.core.transform.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark20_operator_join {

  def main(args: Array[String]): Unit = {

    val datas1 = List(("a", 2),("b", 3),("b", 4))
    val datas2 = List(("a", 1),("b", 5),("b", 6),("c", 6))

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(datas1, 2)
    val rdd2: RDD[(String, Int)] = sc.makeRDD(datas2, 2)

    // 相同的key关联在一起，得到集合
    // 类似sql中的内连接
    // 同一个key之间存在笛卡尔积
    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

    //(b,(3,5))
    //(b,(3,6))
    //(b,(4,5))
    //(b,(4,6))
    //(a,(2,1))
    joinRDD.collect().foreach(println)

    sc.stop
  }

}