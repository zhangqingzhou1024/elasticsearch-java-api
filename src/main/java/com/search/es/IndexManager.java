package com.search.es;

import com.search.es.bean.IndexData;
import com.search.es.indexer.mapping.IndexConfig;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * 索引管理工具
 * 一些参数校验咱不处理，待以后进行优化
 *
 * @author zqz
 */
public class IndexManager {
    private Logger logger = Logger.getLogger(this.getClass());

    /**
     * 每批次最大删除个数
     */
    private static final int MAX_BATCH_DEL_SIZE = 2048;

    /**
     * 创建 index  type
     * 添加 mapping 数据结构   demo
     */
    public void addMapping(IndexConfig indexConfig) {
        Map<String, Object> settings = new HashMap<>(8);
        //分片数量
        settings.put("number_of_shards", indexConfig.getShardsNumber());
        //复制数量
        settings.put("number_of_replicas", indexConfig.getReplicasNumber());
        //刷新时间
        settings.put("refresh_interval", indexConfig.getRefreshInterval());
        //刷新时间
        settings.put("max_result_window", indexConfig.getMaxResultWindow());

        //在本例中主要得注意,ttl及timestamp如何用java ,这些字段的具体含义,请去到es官网查看
        //ik_max_word
        TransportClient client = TransportClientFactory.getEsClient();
        boolean existIndex = isIndexExist(indexConfig.getIndexName());
        if (existIndex) {
            if (indexConfig.getDelHistoryIndex()) {
                System.out.println("索引已建立，进行删除！！！");
                delIndex(indexConfig.getIndexName());
            } else {
                System.out.println("索引已建立，沿用之前索引");
                return;
            }
        }
        CreateIndexRequestBuilder cib = client.admin().indices()
                .prepareCreate(indexConfig.getIndexName());
        cib.setSettings(settings);
        try {
            cib.addMapping(indexConfig.getTypeName(), indexConfig.getMappingJson());
            cib.execute().actionGet();
            System.out.println("创建索引 -> " + indexConfig);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 批量建索引
     *
     * @param dataList 数据列表
     * @return 失败原因
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
     * @param dataList 数据列表
     * @return 更新失败原因
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
     * @param index       索引名
     * @param type        类型
     * @param mappingJson mappingJsonData
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
        Map<String, Object> setting = new HashMap<>(8);
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
     * @param indexName 索引明
     * @return 是否存在
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
     * @param indexName 索引名称
     * @param typeName  类型名称
     * @return 操作结果
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
     * @param indexName 索引名称
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
            isSuccessed = false;
        }
        return isSuccessed;
    }

    /**
     * 删除索引
     *
     * @param indices 要删除的索引名
     */
    public void deleteIndex(String[] indices) {
        if (indices == null || indices.length == 0) {
            throw new NullPointerException("elastic index must be not null");
        }
        TransportClient esClient = TransportClientFactory.getEsClient();
        DeleteIndexResponse deleteIndexResponse =
                esClient
                        .admin()
                        .indices()
                        .prepareDelete(indices)
                        .get();

        // true表示成功
        boolean acknowledged = deleteIndexResponse.isAcknowledged();
        System.out.println("indices 删除结果为" + acknowledged);
    }

    /**
     * 根据文档id 进行删除
     *
     * @param indexName 索引名
     * @param typeName  类型名
     * @param docId
     */
    public void deleteDataByDocId(String indexName, String typeName, String docId) {

        // 先indexName  typename  docID
        TransportClientFactory.getEsClient()
                .prepareDelete(indexName, typeName, docId)
                .execute().actionGet();

    }


    /**
     * 删除信息
     * 根据 entity 中 id 数组 先查询再删除
     *
     * @param ids 每次长度不能超过 2048个
     */
    public void deleteInfoByIds(String indexName, String typeName, long[] ids) {
        // 非空校验
        if (ArrayUtils.isEmpty(ids)) {
            throw new NullPointerException("ids must not null");
        }
        // 长度校验
        if (ids.length > MAX_BATCH_DEL_SIZE) {
            throw new IllegalArgumentException("每次删除ids的长度不能超过" + MAX_BATCH_DEL_SIZE + "个");
        }

        TransportClient esClient = TransportClientFactory.getEsClient();
        // 先查询 再 删除  inQuery("id", ids);
        QueryBuilder qb = QueryBuilders.termsQuery("id", ids);
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        SearchResponse response = esClient.prepareSearch(indexName)
                .setTypes(typeName)
                .setQuery(qb)
                .setSize(MAX_BATCH_DEL_SIZE)
                .execute().actionGet();

        SearchHit[] hits = response.getHits().getHits();
        if (hits.length > 0) {
            // 开启批量删除
            BulkRequestBuilder bulkfresh = esClient.prepareBulk();
            for (SearchHit searchHit : hits) {
                DeleteRequest deleteRequest = new DeleteRequest(indexName, typeName, searchHit.getId());
                bulkfresh.add(deleteRequest);
            }
            // 执行
            bulkfresh.execute().actionGet();
        }
    }

    /**
     * 根据查询条件删除文档
     *
     * @param indexName 索引名称
     * @param typeName  类型名称
     * @param qb        查询条件
     */
    public void deleteInfoByQuery(String indexName, String typeName, QueryBuilder qb) {
        int allHitSize = 0;
        TransportClient esClient = TransportClientFactory.getEsClient();
        SearchResponse response = esClient.prepareSearch(indexName)
                .setTypes(typeName)
                .setQuery(qb)
                .setSize(MAX_BATCH_DEL_SIZE)
                .execute().actionGet();

        // 名字查询条件的数据
        SearchHit[] hits = response.getHits().getHits();
        if (hits.length > 0) {
            // 开启批量删除
            BulkRequestBuilder bulkfresh = esClient.prepareBulk();
            for (SearchHit searchHit : hits) {
                DeleteRequest deleteRequest = new DeleteRequest(indexName, typeName, searchHit.getId());
                // 收集
                bulkfresh.add(deleteRequest);
            }
            // 执行
            bulkfresh.execute().actionGet();
        }
        // 刷新比较慢
        System.out.println("删除:" + hits.length + "条！");
        allHitSize = hits.length;
        if (allHitSize == MAX_BATCH_DEL_SIZE) {
            deleteInfoByQuery(indexName, typeName, qb);
        }
    }


    /**
     * 对某个字段进行更新
     * HashMap<String, Object> editMap = new HashMap<>();
     * editMap.put("state", 3);
     * editMap.put("actionTime", 666666);
     * setDoc("actionTime", 10000)
     * 每次只能改一个,后面的会覆盖前面的
     *
     * @param docId 文档ID
     */
    public static void updateDataInField(String indexName, String typeName, String docId, Map<String, Object> editMap) {
        TransportClient esClient = TransportClientFactory.getEsClient();

        // 只更新某些字段
        esClient.prepareUpdate(indexName, typeName, docId)
                .setDoc(editMap)
                .execute().actionGet();

    }

}
