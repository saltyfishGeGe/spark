package com.bigdata.spark.core.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayOps

object Spark05_Test_top10_improve03 {

  def main(args: Array[String]): Unit = {
    // 测试数据datas/user_visit_action.txt
    // 元数据：
    // 日期，用户ID, SessionID, 页面ID, 时间戳，搜索关键字,点击品类ID, 产品ID,下单品类ID，产品ID，支付品类ID，产品ID,城市ID
    // 列分隔符为_    单列中多个值使用,分隔
    // 搜索，点击，下单，支付，四种行为同时只能存在一种

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("practice01")

    val sc = new SparkContext(sparkConf)

    val startTime: Long = System.currentTimeMillis()

    val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")

    val myAcc = new Top10Accumulator

    sc.register(myAcc, "top10Acc")

    val accList: RDD[(String, String)] = rdd.flatMap(line => {
      val columns: ArrayOps.ofRef[String] = line.split("_")
      if (columns(6) != "-1") {
        // 点击，直接构造成最终数据结构
        List((columns(6), "click"))

      } else if (columns(8) != "null") {
        val ids: ArrayOps.ofRef[String] = columns(8).split(",")
        // 下单，需要打散该列多个值，再构造
        ids.map((_, "order"))

      } else if (columns(10) != "null") {
        val ids: ArrayOps.ofRef[String] = columns(10).split(",")
        // 支付，需打散该列多个值在构造
        ids.map((_, "pay"))

      } else {
        // 不符合数据返回空
        Nil
      }
    })

    accList.foreach(line => {
      myAcc.add(line._1, line._2)
    })

    val result: List[HotBean] = myAcc.value.map(_._2).toList.sortWith((left, right) => {
      if (left.clickNum > right.clickNum) {
        true
      } else if (left.clickNum == right.clickNum) {
        if (left.orderNum > right.orderNum) {
          true
        } else if (left.orderNum == right.orderNum) {
          if (left.payNum > right.payNum) {
            true
          } else {
            false
          }
        }
        false
      }
      false
    }).reverse.take(10)

    result.foreach(println)

    val endTime: Long = System.currentTimeMillis()
    println("耗时：" + endTime.-(startTime))

    sc.stop()
  }

  // 若没有var定义,变量不存在默认的get/set方法，var修饰后会对成员变量提供getter/setter
  // Error:(137, 22) value += is not a member of Int
  //  Expression does not convert to assignment because receiver is not assignable.
  //          h.clickNum += hot.clickNum
  case class HotBean(
    var cid : String,
    var clickNum : Int,
    var orderNum : Int,
    var payNum : Int
  )

  /**
    *
    * IN: (ID, 操作类型)
    * OUT: 输出一个map
    */
  class Top10Accumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotBean]]{

    private var _map = mutable.Map[String, HotBean]()

    override def isZero: Boolean = _map.isEmpty

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotBean]] = this

    override def reset(): Unit = _map = mutable.Map[String, HotBean]()

    override def add(v: (String, String)): Unit = {
      val hot: HotBean = this._map.getOrElse(v._1, HotBean(v._1, 0, 0, 0))

      v._2 match {
        case "click" => {
          this._map.update(hot.cid, HotBean(hot.cid, hot.clickNum.+(1), hot.orderNum, hot.payNum))
        }
        case "order" => {
          this._map.update(hot.cid, HotBean(hot.cid, hot.clickNum, hot.orderNum.+(1), hot.payNum))
        }
        case "pay" => {
          this._map.update(hot.cid, HotBean(hot.cid, hot.clickNum, hot.orderNum, hot.payNum.+(1)))
        }
      }

    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotBean]]): Unit = {
//      other.value.values.foreach(hot => {
//
//        val h: HotBean = this._map.getOrElse(hot.cid, HotBean(hot.cid, 0,0,0))
//        h.clickNum += hot.clickNum
//        h.orderNum += hot.orderNum
//        h.payNum += hot.payNum
//
//        this._map.update(h.cid, h)
//      })

      other.value.foreach{
        case (cid, hot) => {
          val h: HotBean = this._map.getOrElse(cid, HotBean(cid, 0,0,0))

          h.clickNum += hot.clickNum
          h.orderNum += hot.orderNum
          h.payNum += hot.payNum
          this._map.update(cid, h)
        }
      }

    }

    override def value: mutable.Map[String, HotBean] = _map
  }

}
