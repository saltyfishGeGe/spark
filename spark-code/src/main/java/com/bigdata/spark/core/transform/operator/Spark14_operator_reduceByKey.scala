package com.bigdata.spark.core.transform.operator

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark14_operator_reduceByKey {

  def main(args: Array[String]): Unit = {

    val datas = List(("a", 1), ("a", 2), ("b", 1), ("c", 2), ("c", 3))

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(datas, 2)


    // 只有key满足两个以上才会参与运算
    //x= 1, y = 2
    //x= 2, y = 3
    //(b,1)
    //(a,3)
    //(c,5)
    rdd.reduceByKey((x,y) => {
      println(s"x= ${x}, y = ${y}")
      x + y
    }).collect().foreach(println)

    sc.stop
  }

}