package com.atguigu.write;

import com.atguigu.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

public class Bulk_Write_WithJavaBean {
    public static void main(String[] args) throws IOException {

        //1.创建客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.设置连接属性
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //3.获取连接
        JestClient jestClient = jestClientFactory.getObject();

        //4.批量写入数据
        Movie movie104 = new Movie("104", "星球大战");
        Movie movie105 = new Movie("105", "战狼");
        Movie movie106 = new Movie("106", "上海堡垒");

        Index index104 = new Index.Builder(movie104).id("1004").build();
        Index index105 = new Index.Builder(movie105).id("1005").build();
        Index index106 = new Index.Builder(movie106).id("1006").build();

        Bulk bulk = new Bulk.Builder()
                .addAction(index104)
                .addAction(index105)
                .addAction(index106)
                .defaultType("_doc")
                .defaultIndex("movie_test1")
                .build();
        jestClient.execute(bulk);

        //关闭连接
        jestClient.shutdownClient();
    }
}
