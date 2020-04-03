package com.search.es.indexer.mapping;

import lombok.Data;

import java.io.Serializable;

/**
 * 索引配置
 *
 * @author zqz
 * 2018 07 18
 */
@Data
public class IndexConfig implements Serializable {
    /**
     * 该表对应索引名称，默认和数据库名称一致
     */
    private String indexName;
    /**
     * 该表对应索引type名称，默认和表名一致
     */
    private String typeName;
    /**
     * 主分片个数
     */
    private Integer shardsNumber;
    /**
     * 副本个数
     */
    private Integer replicasNumber;
    /**
     * 检索 最大结果集
     */
    private Long maxResultWindow;
    /**
     * 刷新时间间隔
     */
    private String refreshInterval;
    /**
     * 是否删除调已经存在调索引数据
     */
    private Boolean delHistoryIndex;
    /**
     * 索引map映射关系
     */
    private String mappingJson;
}
