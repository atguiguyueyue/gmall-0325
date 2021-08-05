package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.constants.GmallConstants;
import com.atguigu.gmallpublisher.bean.Option;
import com.atguigu.gmallpublisher.bean.Stat;
import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmallpublisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.glassfish.jersey.message.internal.JerseyLink;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;

    @Autowired
    private OrderMapper orderMapper;

    @Autowired
    private JestClient jestClient;

    @Override
    public Integer getSelectDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getSelectDauHour(String date) {
        //1.获取Mapper查出来的数据
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //2.遍历List集合拿出每一个map将其重组成新的map
        HashMap<String, Long> resultMap = new HashMap<>();
        for (Map map : list) {
            resultMap.put((String) map.get("LH"), (Long) map.get("CT"));
        }
        return resultMap;
    }

    @Override
    public Double getSelectGmvTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getSelectGmvHour(String date) {
        //1.获取通过sql查询出来的数据
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);

        //2.创建新的map集合用来改变数据结构
        HashMap<String, Double> result = new HashMap<>();
        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"), (Double) map.get("SUM_AMOUNT"));
        }
        return result;
    }

    @Override
    public Map getSaleDetail(String date, Integer start, Integer size, String keyWord) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤 匹配
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt", date));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name", keyWord).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);

        //  性别聚合
        TermsBuilder genderAggs = AggregationBuilders.terms("groupby_user_gender").field("user_gender").size(2);
        searchSourceBuilder.aggregation(genderAggs);

        //  年龄聚合
        TermsBuilder ageAggs = AggregationBuilders.terms("groupby_user_age").field("user_age").size(100);
        searchSourceBuilder.aggregation(ageAggs);

        // 行号= （页面-1） * 每页行数
        searchSourceBuilder.from((start - 1) * size);
        searchSourceBuilder.size(size);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstants.DETAIL_INDEX_NAME_PREFIXES+"0325").addType("_doc").build();

        SearchResult searchResult = jestClient.execute(search);

        //TODO 1.总数
        Long total = searchResult.getTotal();

        //TODO 2.数据明细
        //创建list集合用来存放明细数据
        ArrayList<Map> details = new ArrayList<>();
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            details.add(hit.source);
        }

        //TODO 3.获取聚合组数据
        MetricAggregation aggregations = searchResult.getAggregations();
        //年龄聚合组
        TermsAggregation groupby_user_age = aggregations.getTermsAggregation("groupby_user_age");
        List<TermsAggregation.Entry> buckets1 = groupby_user_age.getBuckets();
        //20岁以下的个数
        Long low20Count = 0L;
        //30岁以上的个数
        Long up30Count = 0L;
        for (TermsAggregation.Entry entry : buckets1) {
            if (Integer.parseInt(entry.getKey())<20){
                low20Count += entry.getCount();
            }
            if (Integer.parseInt(entry.getKey())>=30){
                up30Count += entry.getCount();
            }
        }
        //20岁以下的占比
        double low20Ratio = Math.round(low20Count * 1000D / total) / 10D;
        //30岁以上的占比
        double up30Ratio = Math.round(up30Count * 1000D / total) / 10D;
        //20到30的占比
        double up20Low30Ratio = Math.round((100 - low20Ratio - up30Ratio) * 10D) / 10D;
        //创建年龄相关的Option对象
        Option low20Opt = new Option("20岁以下", low20Ratio);
        Option up20Low30Opt = new Option("20岁到30岁", up20Low30Ratio);
        Option up30Opt = new Option("30岁及30岁以上", up30Ratio);

        //创建存放年龄有关的Option的List
        ArrayList<Option> ageList = new ArrayList<>();
        ageList.add(low20Opt);
        ageList.add(up20Low30Opt);
        ageList.add(up30Opt);

        //创建年龄相关的Stat对象
        Stat ageStat = new Stat(ageList, "用户年龄占比");

        //性别聚合组
        TermsAggregation groupby_user_gender = aggregations.getTermsAggregation("groupby_user_gender");
        List<TermsAggregation.Entry> buckets = groupby_user_gender.getBuckets();
        //男生的个数
        Long maleCount = 0L;
        for (TermsAggregation.Entry bucket : buckets) {
            if (bucket.getKey().equals("M")){
                maleCount += bucket.getCount();
            }
        }
        //男生所占比例
        double maleRatio = Math.round(maleCount * 1000D / total) / 10D;
        //女生占比
        double femaleRatio = Math.round((100 - maleRatio) * 10D) / 10D;

        //创建跟性别相关的Option对象
        Option maleOption = new Option("男", maleRatio);
        Option femaleOption = new Option("女", femaleRatio);

        //创建存放性别有关的Option的List
        ArrayList<Option> genderOptions = new ArrayList<>();
        genderOptions.add(maleOption);
        genderOptions.add(femaleOption);

        //创建性别相关的Stat对象
        Stat genderStat = new Stat(genderOptions, "用户性别占比");

        //创建存放Stat对象的list集合
        ArrayList<Stat> stats = new ArrayList<>();
        stats.add(ageStat);
        stats.add(genderStat);

        //创造存放最终结果的Map集合
        HashMap<String, Object> result = new HashMap<>();
        result.put("total", total);
        result.put("stat", stats);
        result.put("detail", details);

        return result;
    }
}
