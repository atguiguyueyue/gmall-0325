package com.atguigu.handler

import com.atguigu.bean.StartUpLog
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {
  /**
    * 将mid保存到redis
    * @param startUpLogDStream
    */
  def saveToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=> {
        //1.创建redis连接
        val jedis: Jedis = new Jedis("hadoop102", 6379)

        partition.foreach(StartUpLog=>{
          //redisKey
          val redisKey: String = "DAU:"+StartUpLog.logDate
          jedis.sadd(redisKey,StartUpLog.mid)
        })
        //关闭连接
        jedis.close()
      })
    })
  }

}
