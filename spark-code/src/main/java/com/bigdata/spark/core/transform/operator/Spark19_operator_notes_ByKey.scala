package com.bigdata.spark.core.transform.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark19_operator_notes_ByKey {

  def main(args: Array[String]): Unit = {

    val datas = List(("a", 1),("a", 2),("b", 3),("b", 4),("b", 5),("a", 6))

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(datas, 2)

    /**
      * xxxByKey小结
      * 代码调用底层，均是调用combineByKeyWithClassTag方法，可从方法传参上看出区别
      */

    /**
       combineByKeyWithClassTag[V]((v: V) => v // 第一个值不会参与计算
                                  , func  // 分区内计算规则
                                  , func  // 分区间计算规则
                                  , partitioner
      )
      */
    rdd.reduceByKey(_+_)


    /**
      combineByKeyWithClassTag[U]((v: V) => cleanedSeqOp(createZero(), v) // 初始值和分区内第一个数据的分区内计算
                                  , cleanedSeqOp  // 分区内计算规则
                                  , combOp        // 分区间计算规则
                                  ,partitioner
      )
      */
    rdd.aggregateByKey(0)(_+_, _+_)



    /**
      combineByKeyWithClassTag[V]((v: V) => cleanedFunc(createZero(), v) // 初始值和分区内第一个数据的分区内计算
                                , cleanedFunc  // 分区内计算规则
                                , cleanedFunc  // 分区间计算规则
                                , partitioner
      )
      */
    rdd.foldByKey(0)(_+_)

    /**
      combineByKeyWithClassTag(createCombiner // 相同key的第一条数据进行处理的函数
                              , mergeValue  // 分区内计算规则
                              , mergeCombiners  // 分区间计算规则
      )
      */
    // TODO combineByKey使用时由于初始值是通过转换得来，分区内、分区间计算函数的参数都要给定类型，否则会报错
    rdd.combineByKey(t => t,
      (t1: Int, t2: Int) => t1 + t2,
      (t1: Int, t2: Int) => t1 + t2
    )

    sc.stop
  }

}