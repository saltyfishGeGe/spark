package com.bigdata.spark.sql.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StructField, StructType}

object Spark03_sql_UDAF {

  def main(args: Array[String]): Unit = {

    val conf : SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")

    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    /**
      * 使用DataFrame时，涉及转换操作，需要引入隐式转换规则，否则无法使用
      */

    val df = sparkSession.read.json("datas/user.json")
    df.createOrReplaceTempView("user")

    sparkSession.udf.register("avgAge", new MyUDAF)

    sparkSession.sql("select avgAge(age) from user").show()

    sparkSession.close()
  }



  // 自定义UDAF
  class MyUDAF extends UserDefinedAggregateFunction {

    // 输入字段类型
    override def inputSchema: StructType = {
      StructType(Array(StructField("age", IntegerType, true)))
    }

    // 缓冲保存的数据类型
    override def bufferSchema: StructType = {
      StructType(
        Array(StructField("total", IntegerType, true), StructField("count", IntegerType, true))
      )
    }

    // 输出结果的数据类型
    override def dataType: DataType = IntegerType

    // 函数稳定性，即传入相同的值，结果是否相同
    override def deterministic: Boolean = true

    // 缓冲区初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer.update(0, 0) // 第一个值初始化
        buffer.update(1, 0) // 第二个值初始化
    }

    // 更新缓冲区数据
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(0, buffer.getInt(0) + input.getInt(0))
      buffer.update(1, buffer.getInt(1) + 1)
    }

    // 缓冲区数据合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      // 更新第一个buffer
      buffer1.update(0, buffer1.getInt(0) + buffer2.getInt(0))
      buffer1.update(1, buffer1.getInt(1) + buffer2.getInt(1))
    }

    // 计算逻辑
    override def evaluate(buffer: Row): Any = {
      buffer.getInt(0) / buffer.getInt(1)
    }
  }

}
