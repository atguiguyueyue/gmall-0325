package com.atguigu.handler

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.bean.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {
  /**
    * 进行批次内去重
    *
    * @param filterByMidDStream
    */
  def filterByGroup(filterByMidDStream: DStream[StartUpLog]) = {

    //1.对数据进行转换，转换为(k,v)=>(（logdata,mid）,startUpLog)
    val logDateWithMidToLog: DStream[((String, String), StartUpLog)] = filterByMidDStream.mapPartitions(partition => {
      partition.map(log => {
        ((log.logDate, log.mid), log)
      })
    })

    //2.对相同mid的数据进行聚合
    val logDateWithMitToIterLog: DStream[((String, String), Iterable[StartUpLog])] = logDateWithMidToLog.groupByKey()

    //3.对迭代器中的数据进行处理=》排序，取第一个
    val logDateWithMidToListLog: DStream[((String, String), List[StartUpLog])] = logDateWithMitToIterLog.mapValues(iter => {
      iter.toList.sortWith(_.ts < _.ts).take(1)
    })

    //4.将list中的样例类打散出来返回
    logDateWithMidToListLog.flatMap(_._2)
  }

  /**
    * 批次间去重
    *
    * @param startUpLogDStream
    */
  def filterByMid(startUpLogDStream: DStream[StartUpLog], sc: SparkContext) = {
    /*val value: DStream[StartUpLog] = startUpLogDStream.filter(startUplog => {
      //a.创建redis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)

      //redisKey
      val redisKey: String = "DAU:" + startUplog.logDate

      //判断当前批次的mid是否已经在redis
      val boolean: lang.Boolean = jedis.sismember(redisKey, startUplog.mid)

      //关闭连接
      jedis.close()
      !boolean
    })
    value*/
    //优化：方案二，在分区下创建redis连接
    /*    val value1: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partition => {
          //在每个分区下创建连接
          val jedis: Jedis = new Jedis("hadoop102", 6379)
          val iterator: Iterator[StartUpLog] = partition.filter(log => {
            //redisKey
            val redisKey: String = "DAU:" + log.logDate

            //判断当前批次的mid是否已经在redis
            val boolean: lang.Boolean = jedis.sismember(redisKey, log.mid)
            !boolean
          })
          jedis.close()
          iterator
        })
        value1*/

    //优化：方案三，在每个批次下创建redis连接
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val value: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      //获取redis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)

      //获取redis中的数据
      val logDate: String = sdf.format(new Date(System.currentTimeMillis()))
      val redisKey: String = "DAU:" + logDate
      val mids: util.Set[String] = jedis.smembers(redisKey)

      //将redis中查出来的数据广播到Executer端
      val midBc: Broadcast[util.Set[String]] = sc.broadcast(mids)

      val startUpLogRDD: RDD[StartUpLog] = rdd.filter(log => {
        !midBc.value.contains(log.mid)
      })

      jedis.close()
      startUpLogRDD
    })
    value

  }

  /**
    * 将mid保存到redis
    *
    * @param startUpLogDStream
    */
  def saveToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        //1.创建redis连接
        val jedis: Jedis = new Jedis("hadoop102", 6379)

        partition.foreach(StartUpLog => {
          //redisKey
          val redisKey: String = "DAU:" + StartUpLog.logDate
          jedis.sadd(redisKey, StartUpLog.mid)
        })
        //关闭连接
        jedis.close()
      })
    })
  }

}
