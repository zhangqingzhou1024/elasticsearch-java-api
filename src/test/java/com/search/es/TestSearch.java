package com.search.es;

import com.search.es.query.buildsearch.EsSearchBuilder;
import com.search.es.query.common.KeywordsCombineEnum;
import com.search.es.query.common.QueryTypeEnum;

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

        searchBuilder.addKeywordsQuery("content1", "我是", QueryTypeEnum.MUST, KeywordsCombineEnum.AND);
        searchBuilder.execute();

        long total = searchBuilder.getTotal();
        System.out.println("total:" + total);
    }
}
