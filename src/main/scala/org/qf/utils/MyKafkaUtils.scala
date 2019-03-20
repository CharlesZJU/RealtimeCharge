package org.qf.utils

import java.io.InputStream
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object MyKafkaUtils {
  /*获取kafka数据流
  * */
  def getInputDStream(formdbOffset: Map[TopicPartition, Long], streamingContext: StreamingContext,
                      topics: Array[String], kafka_pro_map: Map[String, Object]): InputDStream[ConsumerRecord[String, String]] = {
    // 判断一下，我们要消费的kafka数据是否是第一次消费，之前有没有消费过
    if (formdbOffset.size == 0) {
      KafkaUtils.createDirectStream[String, String](
        streamingContext,
        // 本地化策略
        // 一般都都是这样写，它会将分区数据尽可能的均匀分布给可用的Executor。
        LocationStrategies.PreferConsistent,
        //消费者策略
        // Subscribe: 不可以动态的更改消费的分区，一般都使用在开始读取数据的时候
        // Assign: 它可以消费固定的topic的partition（集合）
        // SubscribePattern: 可以用于在消费过程中增加分区
        ConsumerStrategies.Subscribe[String, String](topics, kafka_pro_map)
      )
    } else {
      // 如果不是第一次消费数据
      KafkaUtils.createDirectStream(
        streamingContext,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Assign[String, String](formdbOffset.keys, kafka_pro_map, formdbOffset)
      )
    }
  }

  /*
  * 获取kafka的topic
  * */
  def getTopicName(propertyFile: String): String = {
    val kafkapro = new Properties()
    val kafka_pro_stream: InputStream = this.getClass.getResourceAsStream(propertyFile)
    kafkapro.load(kafka_pro_stream)
    val topic: String = kafkapro.getProperty("topics")
    kafka_pro_stream.close()
    topic
  }

  /*
  * 从配置文件中获取kafka参数的map
  * */
  def getPropertyMap(propertyFile: String): Map[String, Object] = {
    val kafkapro = new Properties()
    val kafka_pro_stream: InputStream = this.getClass.getResourceAsStream(propertyFile)
    kafkapro.load(kafka_pro_stream)
    val group_id: String = kafkapro.getProperty("group")
    val broker_list: String = kafkapro.getProperty("brokerList")
    val topic: String = kafkapro.getProperty("topics")
    kafka_pro_stream.close()
    // kafka配置参数
    val kafka_pros = Map[String, Object](
      // 指定消费kafka的ip和端口
      "bootstrap.servers" -> broker_list,
      // 设置kafka的解码方式
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group_id,
      // 从头消费
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    kafka_pros
  }

}
