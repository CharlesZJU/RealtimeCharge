package org.qf.services

import java.{lang, util}

import com.mysql.jdbc
import com.mysql.jdbc.Connection
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis

object GeneralSituation extends Service {
  /*更新成功redis中订单量、失败订单量、充值金额、每小时的订单量变化
  * bussinessRst,chargefee, receiveNotifyTime*/
  override def updateData2Redis(jedis: Jedis, args: Any*): Unit = {
    var bussinessRst: String = ""
    var chargefee: Double = 0
    var receiveNotifyTime: String = ""
    var provincecode: String = ""
    if (args(0).isInstanceOf[String]) {
      bussinessRst = args(0).asInstanceOf[String]
    }
    if (args(1).isInstanceOf[Double]) {
      chargefee = args(1).asInstanceOf[Double]
    }
    if (args(2).isInstanceOf[String]) {
      receiveNotifyTime = args(2).asInstanceOf[String]
    }
    val hour: String = receiveNotifyTime.substring(0, 10)
    if (args(3).isInstanceOf[Int]) {
      provincecode = args(3).asInstanceOf[Int].toString
    }

    jedis.incrBy("RTC:orders", 1)
    jedis.hincrBy("RTC:orderPH", hour, 1)
    jedis.hincrBy(s"RTC:orderP$hour", provincecode, 1)

    bussinessRst match {
      case "0000" => {
        jedis.incrBy("RTC:succorder", 1)
        jedis.incrByFloat("RTC:money", chargefee)
        jedis.hincrByFloat("RTC:moneyPH", hour, chargefee)
      }
      case _ => {
        jedis.hincrBy(s"RTC:failorderP$hour", provincecode, 1)
      }
    }
  }

  def updateFailOrder(jedis: Jedis, connection: Connection, session: SparkSession): Unit = {

  }

  def updateOrderMoneyPH(jedis: Jedis, connection: Connection, session: SparkSession): Unit = {
    val orderPH: util.Map[String, String] = jedis.hgetAll("RTC:orderPH")
    val moneyPH: util.Map[String, String] = jedis.hgetAll("RTC:moneyPH")
  }

  override def redis2Mysql(jedis: Jedis, myjdbc:Connection, sparkSession: SparkSession, args: Any*): Unit = {
    updateFailOrder(jedis: Jedis, myjdbc:Connection, sparkSession: SparkSession)
    updateOrderMoneyPH(jedis: Jedis, myjdbc:Connection, sparkSession: SparkSession)
  }
}





