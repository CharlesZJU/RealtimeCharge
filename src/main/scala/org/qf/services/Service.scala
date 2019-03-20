package org.qf.services

import java.sql.Connection

import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis

trait Service {

  def updateData2Redis(jedis: Jedis,args:Any*)


}
