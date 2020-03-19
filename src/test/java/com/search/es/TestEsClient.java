package com.search.es;

import com.search.es.query.buildsearch.TransportClientFactory;
import org.elasticsearch.client.transport.TransportClient;

/**
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
