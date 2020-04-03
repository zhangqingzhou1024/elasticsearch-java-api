package com.search.es;

import com.search.es.bean.IndexBean;
import com.search.es.indexer.indexer.IndexCreater;
import com.search.es.indexer.indexer.IndexData;
import com.search.es.util.JsonHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * 测试添加
 *
 * @author zqz
 * @version 1.0
 * @date 2020-04-04 00:12
 */
public class TestIndexer {
    public static void main(String[] args) throws Exception {
        IndexCreater indexCreater = new IndexCreater();
        IndexData indexData = new IndexData();
        indexData.setIndexName("liziyuan_hope_info");
        indexData.setTypeName("liziyuan_hope_ref");
        indexData.setDataId("2");


        IndexBean indexBean = new IndexBean();
        indexBean.setContent1("我是zqz");
        indexBean.setId("2");

        indexData.setDataJson(JsonHelper.toJson(indexBean));
        List<IndexData> objects = new ArrayList<>();
        objects.add(indexData);
        indexCreater.bulkIndex(objects);
    }
}
