package com.search.es.indexer.indexer;

import com.search.es.TransportClientFactory;
import com.search.es.util.JsonHelper;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Requests;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * 构建索引
 *
 * @author zqz
 */
public class IndexCreater {
    private Logger logger = Logger.getLogger(this.getClass());

    /**
     * 批量建索引
     *
     * @param dataList
     * @return
     */
    public String bulkIndex(List<IndexData> dataList) {
        String failInfo = null;
        if (dataList.size() == 0) {
            return failInfo;
        }

        try {
            BulkRequestBuilder bulkRequestBuilder = TransportClientFactory.getEsClient().prepareBulk();
            for (IndexData data : dataList) {
                // 拼接索引名称 fixed+ dynamic
                String indexName = data.getIndexName().toLowerCase();
                IndexRequestBuilder indexBuilder = TransportClientFactory.getEsClient().prepareIndex(indexName.toLowerCase(), data.getTypeName(), data.getDataId()).setSource(data.getDataJson());
                bulkRequestBuilder.add(indexBuilder);
            }
            BulkResponse response = bulkRequestBuilder.execute().actionGet();
            if (response.hasFailures()) {
                failInfo = response.buildFailureMessage();
                this.logger.warn(response.buildFailureMessage());
            }
        } catch (Exception e) {
            this.logger.error("exception while index data!", e);
            failInfo = e.toString();
        }

        return failInfo;
    }

    /**
     * 批量更新索引
     *
     * @param dataList
     * @return
     */
    public String updateIndex(List<IndexData> dataList) {
        String failInfo = null;
        if (dataList.size() == 0) {
            return failInfo;
        }

        try {
            BulkRequestBuilder bulkRequestBuilder = TransportClientFactory.getEsClient().prepareBulk();
            for (IndexData data : dataList) {
                String typeName = data.getTypeName();
                UpdateRequestBuilder updateBuilder = TransportClientFactory.getEsClient().prepareUpdate(data.getIndexName(), typeName, data.getDataId()).setDoc(data.getDataJson());
                bulkRequestBuilder.add(updateBuilder);
            }
            BulkResponse response = bulkRequestBuilder.execute().actionGet();
            if (response.hasFailures()) {
                failInfo = response.buildFailureMessage();
                this.logger.warn(response.buildFailureMessage());
            }
        } catch (Exception e) {
            this.logger.error("exception while index data!", e);
            failInfo = e.toString();
        }

        return failInfo;
    }

    /**
     * 添type映射关系
     *
     * @param index
     * @param type
     * @param mappingJson
     */
    public void addTypeMapping(String index, String type, String mappingJson) {
        PutMappingRequest mapping = Requests
                .putMappingRequest(index)
                .type(type)
                .source(mappingJson);
        TransportClientFactory.getEsClient().admin().indices().putMapping(mapping).actionGet();
    }

    /**
     * 构建索引
     *
     * @param indexName   索引名称
     * @param shardsNum   碎片数，0表示采用默认配置
     * @param replicasNum 备份数，0表示采用默认配置
     * @param aliasName   索引别名，可为null
     */
    public void createIndex(String indexName, int shardsNum, int replicasNum, String aliasName) {
        Map<String, Object> setting = new HashMap<String, Object>();
        if (shardsNum > 0) {
            setting.put("number_of_shards", shardsNum);
        }
        if (replicasNum > 0) {
            setting.put("number_of_replicas", replicasNum);
        }
        CreateIndexRequest request = Requests.createIndexRequest(indexName).settings(setting);
        if (aliasName != null && aliasName.length() > 0) {
            Alias alias = new Alias(aliasName);
            request = request.alias(alias);
        }
        TransportClientFactory.getEsClient().admin().indices().create(request).actionGet();
    }

    /**
     * 判断索引是否存在
     *
     * @param indexName
     * @return
     */
    public boolean isIndexExist(String indexName) {
        boolean isExist = false;
        IndicesExistsRequest request = Requests.indicesExistsRequest(indexName);
        IndicesExistsResponse response = TransportClientFactory.getEsClient().admin().indices().exists(request).actionGet();
        isExist = response.isExists();
        return isExist;
    }

    /**
     * 指定type是否存在
     *
     * @param indexName
     * @param typeName
     * @return
     */
    public boolean isTypeExist(String[] indexName, String... typeName) {
        boolean isExist = false;
        TypesExistsRequest request = new TypesExistsRequest(indexName, typeName);
        TypesExistsResponse response = TransportClientFactory.getEsClient().admin().indices().typesExists(request).actionGet();
        isExist = response.isExists();
        return isExist;
    }

    /**
     * 删除指定索引
     *
     * @param indexName
     */
    public boolean delIndex(String indexName) {
        boolean isSuccessed = true;
        try {
            DeleteIndexRequest delRequest = Requests.deleteIndexRequest(indexName);
            DeleteIndexResponse response = TransportClientFactory.getEsClient().admin().indices().delete(delRequest).actionGet();
            isSuccessed = response.isAcknowledged();
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("No index")) {
                this.logger.warn("No index");
            } else {
                this.logger.error("index: " + indexName, e);
            }
        }
        return isSuccessed;
    }

}
