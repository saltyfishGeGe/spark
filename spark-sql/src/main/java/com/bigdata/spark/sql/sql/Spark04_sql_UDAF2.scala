package com.bigdata.spark.sql.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, Row, SparkSession, TypedColumn, functions}

object Spark04_sql_UDAF2 {

  def main(args: Array[String]): Unit = {

    val conf : SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")

    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    /**
      * 使用DataFrame时，涉及转换操作，需要引入隐式转换规则，否则无法使用
      */
    import sparkSession.implicits._

    val df = sparkSession.read.json("datas/user.json")
    df.createOrReplaceTempView("user")

    // TODO 此处由于版本为2.12，还不支持Aggregator方式注册udaf。
    //  spark3.0后可通过functions.udaf将aggregator注册到udaf中并在SQL中使用
    //  早期使用Aggregator需在DSL语法中使用

//    sparkSession.udf.register("MyAvgAge", functions.udaf(new MyAvgAge));
//
//    sparkSession.sql("select MyAvgAge(age) from user").show

    val ds: Dataset[User] = df.as[User]

    // 通过aggregator返回列
    val columns: TypedColumn[User, Long] = new MyAvgAge().toColumn

    // ds查询出结果
    ds.select(columns).show()

    sparkSession.close()
  }


  case class User (var username: String, var age: BigInt)

  /**
    * 实现aggregator
    * 定义泛型
    *         IN: User输入
    *         BUFF:缓冲区
    *         OUT:输出
    * 区别于UDAF，此方法中数值均为强类型
    */
  class MyAvgAge extends Aggregator[User, (BigInt, Int), Long]{

    override def zero: (BigInt, Int) = (0L, 0)

    override def reduce(b: (BigInt, Int), a: User): (BigInt, Int) = {
      (b._1 + a.age, b._2 + 1)
    }

    override def merge(b1: (BigInt, Int), b2: (BigInt, Int)): (BigInt, Int) = {
      (b1._1 + b2._1, b1._2 + b2._2)
    }

    override def finish(reduction: (BigInt, Int)): Long = {
      (reduction._1 / reduction._2).toLong
    }

    override def bufferEncoder: Encoder[(BigInt, Int)] = Encoders.product

    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

}
