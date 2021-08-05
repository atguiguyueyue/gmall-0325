package com.atguigu.gmallpublisher.service;

import java.io.IOException;
import java.util.Map;

public interface PublisherService {
    //获取日活总数抽象方法
    public Integer getSelectDauTotal(String date);

    //获取分时数据抽象方法
    public Map getSelectDauHour(String date);

    //获取GMV每日总数的抽象方法
    public Double getSelectGmvTotal(String date);

    //获取GMV分时数据的抽象方法
    public Map getSelectGmvHour(String date);

    //获取并解析ES中数据的抽象方法
    public Map getSaleDetail(String date, Integer start, Integer size, String keyWord) throws IOException;
}
