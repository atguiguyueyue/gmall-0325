package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {
    //获取日活总数抽象方法
    public Integer selectDauTotal(String date);

    //获取分时数据抽象方法
    public List<Map> selectDauTotalHourMap(String date);
}
