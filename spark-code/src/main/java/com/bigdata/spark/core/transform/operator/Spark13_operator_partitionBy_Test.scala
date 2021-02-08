package com.bigdata.spark.core.transform.operator

import com.bigdata.spark.core.test01.MyParitioner
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_operator_partitionBy_Test {

  def main(args: Array[String]): Unit = {

    val datas = List(Tuple2("yang", 1),Tuple2("yang2", "a"),Tuple2(3, 3),Tuple2(4, 4),Tuple2("yang5", "str"))

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[(Any, Any)] = sc.makeRDD(datas, 2)

    rdd.partitionBy(new MyParitioner(3)).saveAsTextFile("output")

    sc.stop
  }

}