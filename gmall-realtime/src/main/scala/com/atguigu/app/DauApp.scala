package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DauApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    //3.获取kafka中的数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)

    //4.将kafka数据转为样例类,并补全LogDate 和 LogHour这两个字段
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])
        val times: String = sdf.format(new Date(startUpLog.ts))

        //补全LogDate
        startUpLog.logDate = times.split(" ")(0)

        //补全LogHour
        startUpLog.logHour = times.split(" ")(1)

        startUpLog
      })
    })
    startUpLogDStream.print()


    //5.批次间去重

    //6.批次内去重

    //7.将去重后的结果（mid）保存到redis
    DauHandler.saveToRedis(startUpLogDStream)

    //8.将最终明细数据保存到Hbase中

    //4.测试kafka中的数据能不能消费的到
//    kafkaDStream.foreachRDD(rdd=>{
//      rdd.foreachPartition(record=>{
//        record.foreach(log=>{
//          println(log.value())
//        })
//      })
//    })




    //最后开启任务并阻塞
    ssc.start()
    ssc.awaitTermination()
  }
}
