package org.qf.utils

import java.util

import org.apache.kafka.common.TopicPartition

object JedisOffset {

  def apply(groupId:String):Map[TopicPartition,Long] ={
    // 创建Map的topic、partition、offset
    var formdbOffset = Map[TopicPartition,Long]()
    // 获取jedis连接
    val jedis = JedisPool.getConnection()
    // 查询redis的所有kafka相关的topic和partition
    val topicpartitionoffset: util.Map[String, String] = jedis.hgetAll(groupId)
    // 导入隐式转换
    import scala.collection.JavaConversions._
    // 将topicpartitionoffset 转换成list处理
    val list: List[(String, String)] = topicpartitionoffset.toList
    // 循序处理数据
    for(topic <- list){
      formdbOffset += (
        new TopicPartition(topic._1.split("[-]")(0),topic._1.split("[-]")(1).toInt) -> topic._2.toLong)
    }
    formdbOffset
  }
}