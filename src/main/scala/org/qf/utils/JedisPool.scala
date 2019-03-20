package org.qf.utils

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object JedisPool {
private val config = new JedisPoolConfig()
  config.setMaxTotal(20)
  config.setMaxIdle(10)
  private val jedisPool = new JedisPool(config,"",6379,10000,"123")
  def getConnection() ={
    jedisPool.getResource
  }
}
