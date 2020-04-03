package com.search.es.indexer.mapping;


import com.search.es.TransportClientFactory;
import com.thoughtworks.xstream.XStream;
import lombok.extern.log4j.Log4j2;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.client.transport.TransportClient;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * 自动创建索引 mapping
 *
 * @author zhangqingzhou
 */
@Log4j2
public class AutoMapping {
    /**
     * 配置文件列表
     */
    private static List<IndexConfig> indexList = null;
    /**
     * mapping文件存放路径
     */
    private static String mappingPath = "config/mapping";

    public AutoMapping() {
        init();
    }

    /**
     * 初始化解析
     */
    private static void init() {
        XStream xStream = new XStream();
        xStream.alias("indexConfig", IndexConfig.class);
        try {
            File dir = new File(mappingPath);
            System.out.println("mapping-size:" + dir.listFiles().length);
            indexList = new ArrayList<IndexConfig>();
            for (File confFile : dir.listFiles()) {
                try {
                    if (confFile.isDirectory() || confFile.getName().contains(".bak")) {
                        continue;
                    }
                    IndexConfig table = (IndexConfig) xStream
                            .fromXML(new FileInputStream(confFile));

                    System.out.println(table.getIndexName() + "-->" + table.getTypeName());
                    indexList.add(table);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
        }
    }

    /**
     * 进行创建
     */
    public synchronized void createMapping() {
        for (IndexConfig indexConfig : indexList) {
            addMapping(indexConfig);
        }
    }

    /**
     * 创建 index  type
     * 添加 mapping 数据结构   demo
     */
    private void addMapping(IndexConfig indexConfig) {
        Map<String, Object> settings = new HashMap<>();
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
        boolean existIndex = isExistIndex(new String[]{indexConfig.getIndexName()});
        if (existIndex) {
            if (indexConfig.getDelHistoryIndex()) {
                System.out.println("索引已建立，进行删除！！！");
                deleteIndex(new String[]{indexConfig.getIndexName()});
            } else {
                System.out.println("索引已建立，沿用之前索引");
                return;
            }
        }
        CreateIndexRequestBuilder cib = client.admin().indices().prepareCreate(indexConfig.getIndexName());
        cib.setSettings(settings);
        try {
            cib.addMapping(indexConfig.getTypeName(), indexConfig.getMappingJson());
            cib.execute().actionGet();
            System.out.println("创建索引 -> " + indexConfig);
        } catch (ElasticsearchException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断 索引是否在集群中 存在
     *
     * @param indices
     * @return
     */
    public static boolean isExistIndex(String[] indices) {
        boolean flag = false;
        if (indices == null || indices.length == 0) {
            throw new NullPointerException("elastic index is not null");
        }
        TransportClient client = TransportClientFactory.getEsClient();
        try {
            if (client != null) {
                flag = client.admin().indices().exists(
                        new IndicesExistsRequest()
                                .indices(indices))
                        .actionGet().isExists();

            }
        } catch (ElasticsearchException e) {
            e.printStackTrace();
        }

        return flag;
    }

    /**
     * 删除索引
     *
     * @param indices 要删除的索引名
     */
    public static void deleteIndex(String[] indices) {
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

    public static void main(String[] args) {
        AutoMapping index = new AutoMapping();
        index.createMapping();
    }
}
