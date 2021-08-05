package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.UserInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis

object UserInfoApp {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UserInfoApp")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))

    //3.分别获取kafka中order_info 和 oredr_detail数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER,ssc)

    //4.将userInfo数据缓存至redis
    kafkaDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        //创建redis连接
        val jedis: Jedis = new Jedis("hadoop102",6379)
        partition.foreach(record=>{
          //转为样例类的目的是为了方便取到userId（非必须）
          val userInfo: UserInfo = JSON.parseObject(record.value(), classOf[UserInfo])
          //redisKey
          val redisKey: String = "UserInfo:"+userInfo.id
          jedis.set(redisKey,record.value())
        })
        jedis.close()
      })
    })

    val userInfoDStream: DStream[UserInfo] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val userInfo: UserInfo = JSON.parseObject(record.value(), classOf[UserInfo])
        userInfo
      })
    })

    userInfoDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
