package com.atguigu.read;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class Read_WithCode {
    public static void main(String[] args) throws IOException {
        //1.创建客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.设置连接属性
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //3.获取连接
        JestClient jestClient = jestClientFactory.getObject();

        //4.读取数据
        //TODO 相当于 {}
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //TODO --------------------------bool----------------------------------------
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        //TODO --------------------------term----------------------------------------
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("sex", "男");

        //TODO --------------------------filter----------------------------------------
        boolQueryBuilder.filter(termQueryBuilder);

        //TODO --------------------------match----------------------------------------
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("favo", "乒乓球");

        //TODO --------------------------must----------------------------------------
        boolQueryBuilder.must(matchQueryBuilder);

        //TODO --------------------------query----------------------------------------
        searchSourceBuilder.query(boolQueryBuilder);

        //TODO --------------------------terms----------------------------------------
        TermsAggregationBuilder groupByClass1 = AggregationBuilders.terms("groupByClass").field("class_id");

        //TODO --------------------------aggs with terms(max)----------------------------------------
        searchSourceBuilder.aggregation(groupByClass1.subAggregation(AggregationBuilders.max("groupByAge").field("age")));

        //TODO --------------------------from----------------------------------------
        searchSourceBuilder.from(0);

        //TODO --------------------------size----------------------------------------
        searchSourceBuilder.size(2);


        Search search = new Search.Builder(searchSourceBuilder.toString()).build();
        SearchResult result = jestClient.execute(search);

        //TODO 获取命中条数
        Long total = result.getTotal();
        System.out.println("命中条数："+total);

        //TODO 获取数据明细
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            //获取索引名
            System.out.println("_index:"+hit.index);
            //获取类型名
            System.out.println("_type"+hit.type);
            //获取docid
            System.out.println("_id:"+hit.id);
            //获取明细数据
            Map source = hit.source;
            for (Object o : source.keySet()) {
                System.out.println(o+":"+source.get(o));
            }
        }

        //TODO 获取聚合组数据
        MetricAggregation aggregations = result.getAggregations();

        //获取按照班级聚合的数据
        TermsAggregation groupByClass = aggregations.getTermsAggregation("groupByClass");

        //获取具体的数据
        List<TermsAggregation.Entry> buckets = groupByClass.getBuckets();

        for (TermsAggregation.Entry bucket : buckets) {
            System.out.println("key:"+bucket.getKey());
            System.out.println("doc_count:"+bucket.getCount());

            //获取按照年龄聚合的数据
            MaxAggregation groupByAge = bucket.getMaxAggregation("groupByAge");
            System.out.println("value:"+groupByAge.getMax());
        }


        //关闭连接
        jestClient .shutdownClient();
    }
}
