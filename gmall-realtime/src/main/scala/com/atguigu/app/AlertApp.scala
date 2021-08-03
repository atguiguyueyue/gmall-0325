package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._

object AlertApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //3.消费kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)

    //4.将json格式的数据转为样例类，并补全字段,返回kv格式的数 k：mid v：样例类
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val midToEventLogDStream: DStream[(String, EventLog)] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])

        val times: String = sdf.format(new Date(eventLog.ts))

        //补全字段
        eventLog.logDate = times.split(" ")(0)
        eventLog.logHour = times.split(" ")(1)

        (eventLog.mid, eventLog)
      })
    })

    //5.开窗 5分钟窗口
    val midToEventLogWindowDStream: DStream[(String, EventLog)] = midToEventLogDStream.window(Minutes(5))

    //6.对相同mid的数据 进行分组 groupBykey
    val midToIterEventLogDStream: DStream[(String, Iterable[EventLog])] = midToEventLogWindowDStream.groupByKey()

    //7.根据条件筛选数据
    val boolToAlertDStream: DStream[(Boolean, CouponAlertInfo)] = midToIterEventLogDStream.mapPartitions(partition => {
      partition.map { case (mid, iter) =>
        //创建set集合用来保存没有浏览商品但是领优惠券的uid
        val uids: util.HashSet[String] = new util.HashSet[String]()
        //创建set集合用来保存领优惠券所涉及的商品id
        val itemIds: util.HashSet[String] = new util.HashSet[String]()
        //创建list集合用来保存用户所涉及的行为
        val events: util.ArrayList[String] = new util.ArrayList[String]()

        //定义标志位用来判断用户是否有浏览商品行为
        var bool: Boolean = true

        //遍历迭代器中每条数据
        breakable {
          iter.foreach(log => {
            //向集合中添加所涉及的行为
            events.add(log.evid)
            if ("clickItem".equals(log.evid)) {
              //有浏览商品
              bool = false
              break()
            } else if ("coupon".equals(log.evid)) {
              //没有浏览商品
              uids.add(log.uid)
              itemIds.add(log.itemid)
            }
          })
        }
        //生成疑似预警日志
        (uids.size() >= 3 && bool, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
      }
    })
    boolToAlertDStream
    
    //8.筛选符合预警需求的数据
    val couponAlertInfoDStream: DStream[CouponAlertInfo] = boolToAlertDStream.filter(_._1).map(_._2)

    couponAlertInfoDStream.print()

    //9.将预警日志写入ES
    couponAlertInfoDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        val list: List[(String, CouponAlertInfo)] = partition.toList.map(log => {
          (log.mid + log.ts / 1000 / 60, log)
        })
        MyEsUtil.insertBulk(GmallConstants.ALERT_INDEX_NAME_PREFIXES,list)
      })
    })

    //10.开启任务，并阻塞
    ssc.start()
    ssc.awaitTermination()
  }
}
