package org.qf.utils

import java.sql.{Connection, DriverManager}
import java.util

/**
  * jdbc连接池
  */
object MyJDBCPool {
  private val max = 10 // 连接池总数
  private val connectionNum = 10 // 每次最大连接数
  private val pool = new util.LinkedList[Connection]() // 连接池
  private var countNum = 0   // 当前连接池已经产生的连接数

  // 获取连接
  def getConnections():Connection={
    // 同步代码块
    AnyRef.synchronized({
      if(pool.isEmpty){
        // 加载驱动
        GetConn()
        for(i <- 1 to connectionNum){
          val conn = DriverManager.getConnection(
            "jdbc:mysql://192.168.48.1:3306/realtimeCharge",
            "root",
            "123"
          )
          pool.push(conn)
          countNum += 1
        }
      }
      pool.poll()
    })
  }

  // 释放连接
  def returnConn(conn:Connection): Unit ={
    pool.push(conn)
  }

  // 加载驱动
  def GetConn(): Unit ={
    // 控制加载
    if(countNum > max && !pool.isEmpty){
      println("不能加载驱动")
      Thread.sleep(2000)
      GetConn()
    }else{
      Class.forName("com.mysql.jdbc.Driver")
    }
  }

}