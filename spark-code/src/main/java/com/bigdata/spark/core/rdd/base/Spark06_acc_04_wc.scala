package com.bigdata.spark.core.rdd.base

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark06_acc_04_wc {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark01_rdd")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(List("hello", "spark", "hive", "spark"))

    val myAcc = new MyAccumulator[String]
    sc.register(myAcc)

    rdd.foreach(num => {
      myAcc.add(num)
    })

    println(myAcc.value)

    sc.stop()
  }

  // 自定义累加器
  class MyAccumulator[String] extends AccumulatorV2[String, mutable.Map[String, Long]]{

    private var _map = mutable.Map[String, Long]()

    // 判断是否为初始状态
    override def isZero: Boolean = _map.isEmpty

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = new MyAccumulator

    override def reset(): Unit = _map.clear()

    override def add(v: String): Unit = {
      val newCount: Long = _map.getOrElse(v, 0L)
      _map.update(v, newCount + 1)
    }

    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      other.value.toList.foreach(kv => {
        val k: String = kv._1
        val cnt: Long = kv._2
        val oldCnt: Long = this._map.getOrElse(k, 0)
        this._map.update(k, cnt + oldCnt)
      })
    }

    override def value: mutable.Map[String, Long] = {
      _map
    }
  }

}
