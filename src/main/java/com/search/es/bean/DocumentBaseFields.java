package com.search.es.bean;

import lombok.Data;

/**
 * 索引文档基本信息字段
 * 用来构建查询
 *
 * @author zqz
 * @version 1.0
 * @date 2020-04-04 19:19
 */
@Data
public class DocumentBaseFields {

    /**
     * 索引名称
     */
    private String indexName;
    /**
     * 类型名称
     */
    private String typeName;

    /**
     * 文档ID
     */
    private String docId;
}
