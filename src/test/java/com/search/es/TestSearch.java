package com.search.es;

import com.google.common.collect.Lists;
import com.search.es.bean.IndexBean;
import com.search.es.query.buildsearch.EsSearchBuilder;
import com.search.es.query.common.KeywordsCombineEnum;
import com.search.es.query.common.QueryTypeEnum;
import org.apache.commons.lang3.ArrayUtils;

import java.lang.reflect.Field;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 测试搜索
 *
 * @author zqz
 * @version 1.0
 * @date 2020-04-04 00:13
 */
public class TestSearch {
    public static void main(String[] args) {
        EsSearchBuilder searchBuilder = new EsSearchBuilder("liziyuan_hope_info");

        Field[] fields = IndexBean.class.getDeclaredFields();
        List<String> fieldArr = Stream.of(fields).map(Field::getName).collect(Collectors.toList());
        searchBuilder.addKeywordsQuery("content1", "中国人", QueryTypeEnum.MUST, KeywordsCombineEnum.AND);
        searchBuilder.execute(fieldArr.toArray(new String[0]));

        long total = searchBuilder.getTotal();

        System.out.println("total:" + total);
        List<IndexBean> resultList = searchBuilder.getResultList(IndexBean.class);
        resultList.forEach(data -> {
            System.out.println(data.getIndexName() + "==> " + data);
        });

    }
}
