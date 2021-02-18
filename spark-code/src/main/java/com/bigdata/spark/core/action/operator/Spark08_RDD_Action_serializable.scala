package com.bigdata.spark.core.action.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_RDD_Action_serializable {

  def main (args: Array[String] ): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("action_operator")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)

    val user = new User();

    // user类若无extends Serializable，则会报出序列化异常
    // Task not serializable
    // java.io.NotSerializableException: com.bigdata.spark.core.action.operator.Spark08_RDD_Action_serializable$User

    // rdd算子中，传递的函数是闭包操作，即内部函数中的内容一定会传递进executor中
    // scala默认会对匿名函数进行检测序列化，即闭包检测功能 --> ClosureCleaner --> ensureSerializable
    rdd.foreach( num => {
      println(num + user.age)
    })

    sc.stop()
  }

  /**
    * 序列化： 算子以外的代码在driver端执行，算子内代码都在executor端执行，算子内使用到算子外的数据，形成闭包效果。
    *         如果算子外的数据无法序列化，就意味着无法传值给executor端执行，所以在执行任务之前会进行序列化检测。
    */

  class User extends Serializable {
    var age: Int = 30
  }


  // 样例类，默认继承序列化接口
  case class User2() {
    var age:Int = 30
  }
}
