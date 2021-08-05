package com.atguigu.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization

import collection.JavaConverters._
object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleDetailApp")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //3.分别获取kafka中order_info 和 oredr_detail数据
    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)

    val detailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)

    //4.将数据转为样例类,并转为kv结构，为了后面join时操作
    val orderInfoDStream = orderInfoKafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        //先将数据转为样例类
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

        //补全字段
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)

        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
        (orderInfo.id, orderInfo)
      })
    })

    val detailDStream = detailKafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
        (orderDetail.order_id, orderDetail)
      })
    })

    //5.将两条流的数据join起来
    //    val value: DStream[(String, (OrderInfo, OrderDetail))] = orderInfoDStream.join(detailDStream)
    val orderIdToinfoOptWithDetailOptDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoDStream.fullOuterJoin(detailDStream)

    //6.使用加缓存的方式将数据关联起来
    val noUserSaleDetailDStream: DStream[SaleDetail] = orderIdToinfoOptWithDetailOptDStream.mapPartitions(partition => {
      implicit val formats = org.json4s.DefaultFormats
      //创建集合用来存放关联起来的结果数据
      val details: util.ArrayList[SaleDetail] = new util.ArrayList[SaleDetail]()
      //创建redis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)

      partition.foreach { case (orderId, (infoOpt, detaiOpt)) =>
        //存放orderInfo的redisKey
        val infoRedisKey: String = "OrderInfo:" + orderId
        //存放orderDetail的redisKey
        val detailRedisKey: String = s"OrderDtail:${orderId}"

        //TODO 1.判断orderInfo是否存在
        if (infoOpt.isDefined) {
          //orderInfo数据存在
          val orderInfo: OrderInfo = infoOpt.get
          //TODO 2.判断orderDetail是否存在
          if (detaiOpt.isDefined) {
            //orderDetail数据存在
            val orderDetail: OrderDetail = detaiOpt.get
            //TODO 3.将两个数据关联起来
            val saleDetail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
            details.add(saleDetail)
          }

          //TODO 4.去对方（orderDetail）缓存中查询是否有能关联上的数据
          //判断redis的key在redis中是否存在
          if (jedis.exists(detailRedisKey)) {
            val orderDetailJsonSet: util.Set[String] = jedis.smembers(detailRedisKey)
            //遍历set集合获取到每个orderDetail数据
            for (elem <- orderDetailJsonSet.asScala) {
              //将json格式的数据转为样例类，为了能够结合起来
              val orderDetail: OrderDetail = JSON.parseObject(elem, classOf[OrderDetail])
              val saleDetail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
              details.add(saleDetail)
            }
          }

          //TODO 5.将自己（orderInfo）写入缓存
          //          JSON.toJSONString(orderInfo)
          val orderInfoJson: String = Serialization.write(orderInfo)
          jedis.set(infoRedisKey, orderInfoJson)
          //设置过期时间
          jedis.expire(infoRedisKey, 30)
        } else {
          //TODO orderInfo不存在orderDetail存在
          if (detaiOpt.isDefined) {
            val orderDetail: OrderDetail = detaiOpt.get
            //TODO 6.去对方（orderInfo）缓存中查找是否有对应的info数据
            if (jedis.exists(infoRedisKey)) {
              val orderInfoJsonStr: String = jedis.get(infoRedisKey)
              //将查出来的orderInfoJson格式的数据转为样例类
              val orderInfo: OrderInfo = JSON.parseObject(orderInfoJsonStr, classOf[OrderInfo])
              val saleDetail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
              details.add(saleDetail)
            } else {
              //TODO 7.orderInfo的key不存在，证明数据关联不上，orderDetail来早了
              //将样例类转为json字符串并存入redis
              val orderDetailJson: String = Serialization.write(orderDetail)
              jedis.sadd(detailRedisKey, orderDetailJson)
              //设置过期时间
              jedis.expire(detailRedisKey, 30)
            }
          }
        }
      }
      jedis.close()
      //将集合转为迭代器返回
      details.asScala.toIterator
    })
//    noUserSaleDetailDStream.print()


    //7.查询userInfo缓存补全用户数据
    val saleDetailDStream: DStream[SaleDetail] = noUserSaleDetailDStream.mapPartitions(partition => {
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      val details: Iterator[SaleDetail] = partition.map(saleDetail => {
        //根据UserInfo的redisKey查询userInfo数据
        val userInfoRedisKey: String = "UserInfo:" + saleDetail.user_id
        //获取userInfoJson字符串数据
        val userInfoJsonStr: String = jedis.get(userInfoRedisKey)
        //将json串转为样例类
        val userInfo: UserInfo = JSON.parseObject(userInfoJsonStr, classOf[UserInfo])
        saleDetail.mergeUserInfo(userInfo)
        saleDetail
      })
      jedis.close()
      details
    })
    saleDetailDStream.print()

    //8.将SaleDetail数据保存至ES
    saleDetailDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        val list: List[(String, SaleDetail)] = partition.toList.map(saleDetail => {
          (saleDetail.order_detail_id, saleDetail)
        })
        MyEsUtil.insertBulk(GmallConstants.DETAIL_INDEX_NAME_PREFIXES+"0325",list)
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
