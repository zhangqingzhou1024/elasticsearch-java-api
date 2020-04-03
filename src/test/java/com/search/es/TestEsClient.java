package com.search.es;

import org.elasticsearch.client.transport.TransportClient;

/**
 * 测试连接
 *
 * @author zqz
 * @version 1.0
 * @date 2020-03-19 15:04
 */
public class TestEsClient {
    public static void main(String[] args) {
        TransportClient esClient = TransportClientFactory.getEsClient();
        System.out.println(esClient);
    }
}
