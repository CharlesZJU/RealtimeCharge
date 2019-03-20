package org.qf.app


import java.lang
import java.sql.Connection


import com.alibaba.fastjson.JSON
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ HasOffsetRanges}
import org.apache.spark.streaming.{ Seconds, StreamingContext}
import org.qf.services.{GeneralSituation}
import org.qf.utils._
import redis.clients.jedis.Jedis

import scala.collection.mutable


object RealtimeChargeApp {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "100")
      // 设置序列化
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // TODO: 不同业务时间长度不同
    val streamingContext = new StreamingContext(sparkConf, Seconds(3))
    //    读入配置文件参数
    val kafka_pro_file = "mykafka.properties"
    val kafka_pro_map: Map[String, Object] = MyKafkaUtils.getPropertyMap(kafka_pro_file)
    // 将Topic放入数组
    val topics: Array[String] = Array(MyKafkaUtils.getTopicName(kafka_pro_file))
    // 获取redis中的kafka偏移量
    var formdbOffset: Map[TopicPartition, Long] = MyJedisOffset(kafka_pro_map.get("group.id").toString)
    // 拉取kafka数据
    val kafka_stream: InputDStream[ConsumerRecord[String, String]]
    = MyKafkaUtils.getInputDStream(formdbOffset, streamingContext, topics, kafka_pro_map)

    //    获取字典文件
    val proDic = mutable.Map[Int, String]()
    streamingContext.sparkContext.textFile("E:\\BigData\\project-flum\\实时项目\\充值平台实时统计分析\\city.txt", 1)
      .foreach(line => {
        val linestr: Array[String] = line.split(" ")
        proDic.put(linestr(0).toInt, linestr(1))
      })
    //   处理数据流
    kafka_stream.foreachRDD(dStreamRDD => {
      //首先我们想获取处理数据的全信息，包括topic partition、offset
      val offsetRange = dStreamRDD.asInstanceOf[HasOffsetRanges].offsetRanges
      // 业务处理
      dStreamRDD.map(line => {
        JSON.parseObject(line.value())
      }).filter(log_json => {
        //          筛选充值通知接口
        log_json.getString("serviceName")
          .equalsIgnoreCase("reChargeNotifyReq") &&
          StringUtils.isNotBlank(log_json.getString("bussinessRst"))
      })
        //        获取业务所需数据
        .foreachPartition(log_json_ite => {
        //获取所需连接
        lazy val jedis: Jedis = MyJedisPool.getConnection()
        lazy val jdbc: Connection = MyJDBCPool.getConnections()
        lazy val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

        //计算分区内的数据
        log_json_ite.foreach(log_json => {
          //            业务结果  0000 成功，其它返回错误编码
          val bussinessRst: String = log_json.getString("bussinessRst")
          val chargefee: Double = log_json.getDouble("chargefee")
          val receiveNotifyTime: String = log_json.getString("receiveNotifyTime")
          val provinceCode: Int = log_json.getIntValue("provinceCode")

          //            交易总量、总额度、成功数、每小时数据
          GeneralSituation.updateData2Redis(jedis, bussinessRst, chargefee, receiveNotifyTime, provinceCode)
        })

        // TODO:  从redis中更新数据到mysql

      })
    })


  }

}
