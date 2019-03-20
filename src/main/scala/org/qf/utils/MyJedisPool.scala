package org.qf.utils

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object MyJedisPool {
private val config = new JedisPoolConfig()
  config.setMaxTotal(20)
  config.setMaxIdle(10)
  private val jedisPool = new JedisPool(config,"192.168.42.120",6379,10000)
  def getConnection() ={
    jedisPool.getResource
  }
}
