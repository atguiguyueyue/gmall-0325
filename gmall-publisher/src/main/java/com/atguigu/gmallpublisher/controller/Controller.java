package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class Controller {

    @Autowired
    private PublisherService publisherService;

    //日活总数
    @RequestMapping("realtime-total")
    public String getDauTotal(@RequestParam("date") String date) {
        //1.获取日活总数数据
        Integer dauTotal = publisherService.getSelectDauTotal(date);

        //2.创建list集合用来存放结果数据
        ArrayList<Map> result = new ArrayList<>();

        //3.创建map集合用来存放具体数据
        //存放新增日活的map集合
        HashMap<String, Object> dauMap = new HashMap<>();

        //存放新增设备的map集合
        HashMap<String, Object> devMap = new HashMap<>();

        //存放GMV的map集合
        HashMap<String, Object> gmvMap = new HashMap<>();

        //4.将数据封装到Map集合中
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        devMap.put("id", "new_mid");
        devMap.put("name", "新增设备");
        devMap.put("value", 233);

        gmvMap.put("id", "order_amount");
        gmvMap.put("name", "新增交易额");
        gmvMap.put("value", publisherService.getSelectGmvTotal(date));


        //5.将map集合放入List集合
        result.add(dauMap);
        result.add(devMap);
        result.add(gmvMap);

        return JSONObject.toJSONString(result);
    }

    //获取分时数据方法
    @RequestMapping("realtime-hours")
    public String getDauHour(@RequestParam("id") String id,
                             @RequestParam("date") String date) {
        //1.获取service返回的数据
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();

        Map todayMap=null;
        Map yesterdayMap=null;

        //根据id来判断获取的是谁的数据
        if ("order_amount".equals(id)) {
            //获取gmv的分时数据
            //1.1获取今天的数据
            todayMap = publisherService.getSelectGmvHour(date);
            //1.2获取昨天的数据
            yesterdayMap = publisherService.getSelectGmvHour(yesterday);
        }else {
            //获取日活的分时数据
            //1.1获取今天的数据
            todayMap = publisherService.getSelectDauHour(date);
            //1.2获取昨天的数据
            yesterdayMap = publisherService.getSelectDauHour(yesterday);
        }

        //2.创建存放最终结果的map集合
        HashMap<String, Map> result = new HashMap<>();

        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);

        return JSONObject.toJSONString(result);
    }


}
