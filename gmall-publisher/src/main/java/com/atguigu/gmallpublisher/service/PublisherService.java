package com.atguigu.gmallpublisher.service;

import java.util.Map;

public interface PublisherService {
    //获取日活总数抽象方法
    public Integer getSelectDauTotal(String date);

    //获取分时数据抽象方法
    public Map getSelectDauHour(String date);
}
