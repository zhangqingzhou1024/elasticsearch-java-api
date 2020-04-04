package com.search.es.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 待建索引数据/待更新索引数据，需指定dataid！
 *
 * @author zqz
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class IndexData {
    /**
     * 索引名称
     */
    private String indexName;
    /**
     * 类型名称
     */
    private String typeName;
    /**
     * 数据
     */
    private String dataJson;
    /**
     * docId 更新索引时需指定！
     */
    private String dataId;

}
