package com.bigdata.spark.sql.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Spark01_sql_Basic {

  def main(args: Array[String]): Unit = {

    val conf : SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")

    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    /**
      * 使用DataFrame时，涉及转换操作，需要引入隐式转换规则，否则无法使用
      */
    import sparkSession.implicits._

//    val df: DataFrame = sparkSession.read.json("datas/user.json")
//
//    df.createOrReplaceGlobalTempView("user")

    // DataFrame => sql
//    sparkSession.sql("select * from global_temp.user").show()
//    sparkSession.sql("select avg(age) from global_temp.user").show()
//    sparkSession.sql("select username from global_temp.user").show()


    // DataFreme => DSL
//    df.select("age", "username").show()
//    df.select("age").groupBy("age").count().show()
//    df.select('age + 1).show()
//    df.select($"age" + 1).show()

    // RDD <=> DataFrame
    val rdd: RDD[(String, Int)] = sparkSession.sparkContext.makeRDD(List(("zhangsan",10), ("lisi", 20)))
    val df2: DataFrame = rdd.toDF("username", "age")
    val rdd2 = df2.rdd

    // DataFrame <=> DataSet
    val ds: Dataset[Person] = df2.as[Person]
    ds.show()
    val df3: DataFrame = ds.toDF()
    df3.show()

    // RDD <=> DataSet
    val personDS: Dataset[Person] = rdd.map {
      case (username, age) => {
        Person(age, username)
      }
    }.toDS()
    personDS.show()

    val rdd3 = personDS.rdd
    println(rdd3)

    sparkSession.close()
  }

  case class Person(
                  age: Long,
                  username: String
                  )
}
