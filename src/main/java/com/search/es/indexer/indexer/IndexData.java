package com.search.es.indexer.indexer;

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
    private String indexName;
    private String typeName;
    private String dataJson;
    /**
     * 更新索引时需指定！
     */
    private String dataId;

}
