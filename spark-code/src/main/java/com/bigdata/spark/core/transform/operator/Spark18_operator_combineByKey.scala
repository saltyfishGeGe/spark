package com.bigdata.spark.core.transform.operator

import com.bigdata.spark.core.scala_test.MyParitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark18_operator_combineByKey {

  def main(args: Array[String]): Unit = {

    val datas = List(("a", 1),("a", 2),("b", 3),("b", 4),("b", 5),("a", 6))

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(datas, 2)

    // 第一个参数： 设置值转换规则，将所有值转换成指定的格式
    // 第二个参数： 分区内计算逻辑
    // 第三个参数： 分区间计算逻辑
    // 与aggregateByKey的区别是，combineByKey是设定了分区内取值时的转换规则，则并非构造一个初始的值进行计算
    // ！！combineByKey在使用过程中，参数泛型最好明确指定，否则会有一些乱七八糟的报错提示，这是由于首个参数是计算出来的，类型无法推断出来
    val combineRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(
      v => (v, 1),
      (t1: (Int, Int), t2: Int) => {
        (t1._1 + t2, t1._2 + 1)
      },
      (t1: (Int, Int), t2: (Int, Int)) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )

    combineRDD.foreach(println)

    combineRDD.mapValues(re => {re._1 / re._2}).collect().foreach(println)

    sc.stop
  }

}