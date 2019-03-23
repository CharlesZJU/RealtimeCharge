package org.qf.services



import com.mysql.jdbc.Connection
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis

trait Service {

  def updateData2Redis(jedis: Jedis,args:Any*)

  def redis2Mysql(jedis: Jedis,myjdbc:Connection,sparkSession: SparkSession,args:Any*)


}
