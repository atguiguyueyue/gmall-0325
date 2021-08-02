package com.atguigu.write;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;

public class Single_Write {
    public static void main(String[] args) throws IOException {

        //1.创建客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.设置连接属性
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //3.获取连接
        JestClient jestClient = jestClientFactory.getObject();

        //4.写入数据
        Index index = new Index.Builder("{\n" +
                "  \"id\":\"102\",\n" +
                "  \"movie\":\"复仇者联盟\"\n" +
                "}")
                .id("1002")//文档id
                .index("movie_test1")//索引名
                .type("_doc")//类型名
                .build();

        jestClient.execute(index);

        //关闭连接
        jestClient.shutdownClient();
    }
}
