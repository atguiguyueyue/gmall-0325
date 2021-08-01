package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {
    //获取GMV每天总数的数据
    public Double selectOrderAmountTotal(String date);

    //获取GMV分时数据
    public List<Map> selectOrderAmountHourMap(String date);
}
