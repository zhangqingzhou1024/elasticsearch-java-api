package com.search.es;

import com.search.es.bean.IndexBean;
import com.search.es.bean.IndexData;
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
        IndexManager indexCreater = new IndexManager();
        IndexData indexData = new IndexData();
        indexData.setIndexName("liziyuan_hope_info");
        indexData.setTypeName("liziyuan_hope_ref");
        indexData.setDataId("3");


        IndexBean indexBean = new IndexBean();
        indexBean.setContent1("我是zqz");
        indexBean.setId("3");

        indexBean.setContent1("中国人你好，加油");
        indexData.setDataJson(JsonHelper.toJson(indexBean));
        List<IndexData> objects = new ArrayList<>();
        objects.add(indexData);
        indexCreater.bulkIndex(objects);
    }
}
