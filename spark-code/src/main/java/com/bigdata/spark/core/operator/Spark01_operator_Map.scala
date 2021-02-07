package com.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_operator_Map {

  def main(args: Array[String]): Unit = {


    val datas: List[Int] = List[Int](1,2,3,4)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator_Map")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(datas)

    def mapFunc(num:Int): Int = {
      num * 2
    }


    val mapRDD: RDD[Int] = rdd.map(mapFunc)

    // val mapRDD2 : RDD[Int] = rdd.map(_ * 2)

    mapRDD.foreach(println)

    mapRDD.collect()

    sc.stop
  }

}
