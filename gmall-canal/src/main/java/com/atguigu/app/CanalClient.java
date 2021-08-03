package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstants;
import com.atguigu.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

public class CanalClient {
    public static void main(String[] args) throws InterruptedException, InvalidProtocolBufferException {
        //1.获取canal连接对象
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");

        while (true) {
            //2.获取连接
            connector.connect();

            //3.订阅数据库
            connector.subscribe("gmall.*");

            //4.获取多个sql执行的结果
            Message message = connector.get(100);

            //5.获取一个sql执行的结果
            List<CanalEntry.Entry> entries = message.getEntries();

            if (entries.size() <= 0) {
                System.out.println("没有数据！休息一会");
                Thread.sleep(5000);
            } else {
                //6.遍历存放entry的list集合获取到每个sql执行的结果
                for (CanalEntry.Entry entry : entries) {
                    //TODO 7.获取表名
                    String tableName = entry.getHeader().getTableName();

                    //8.获取entry类型，根据这个类型来获取具体的数据
                    CanalEntry.EntryType entryType = entry.getEntryType();

                    //9.获取rowData类型
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {

                        //10.获取具体的数据
                        ByteString storeValue = entry.getStoreValue();

                        //11.对数据进行反序列化
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                        //TODO 12.获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();

                        //TODO 13.获取多行数据
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                        hadnle(tableName, eventType, rowDatasList);
                    }

                }
            }

        }
    }

    /**
     * 用来具体解析数据
     *
     * @param tableName
     * @param eventType
     * @param rowDatasList
     */
    private static void hadnle(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        //1.根据表名以及所需要的新增或者变化的条来决定取什么数据
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER);
            //获取订单明细表新增数据
        } else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            //2.获取每一行数据
            saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER_DETAIL);
            //获取用户表新增及变化数据
        } else if ("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType) || CanalEntry.EventType.UPDATE.equals(eventType))) {
            //2.获取每一行数据
            saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_USER);
        }
    }

    private static void saveToKafka(List<CanalEntry.RowData> rowDatasList, String kafkaTopicOrder) {
        //2.获取每一行数据
        for (CanalEntry.RowData rowData : rowDatasList) {
            //3.获取更新之后的数据
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            //4.获取每一列数据
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {
                jsonObject.put(column.getName(), column.getValue());
            }
            System.out.println(jsonObject.toString());
            //5.发送数据到kafka
            //模拟网络震荡
            try {
                Thread.sleep(new Random().nextInt(5)*1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            MyKafkaSender.send(kafkaTopicOrder, jsonObject.toString());
        }
    }
}

