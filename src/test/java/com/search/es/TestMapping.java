package com.search.es;

import com.search.es.indexer.mapping.AutoMapping;

/**
 * 测试添加mapping
 *
 * @author zqz
 * @version 1.0
 * @date 2020-04-04 00:39
 */
public class TestMapping {
    public static void main(String[] args) {
        AutoMapping index = new AutoMapping();
        index.createMapping();



    }
}
