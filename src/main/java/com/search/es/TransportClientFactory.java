package com.search.es;

import com.search.es.bean.ElasticClusterConf;
import com.search.es.util.ConfReader;
import lombok.extern.log4j.Log4j2;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;


/**
 * ES获取client 工具类
 *
 * @author zhangqingzhou
 */
@Log4j2
public class TransportClientFactory {
    private static Object lock = new Object();
    private static volatile TransportClient esClient;

    /**
     * 初始化 elastic 连接
     */
    private static void initEsClient() throws Exception {
        System.out.println("初始化 ES client...");
        try {
            ElasticClusterConf esClusterConfig = ConfReader.getESClusterConfig();
            //设置集群名称
            Settings settings = Settings.builder().put("cluster.name", esClusterConfig.getClusterName()).build();
            esClient = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(esClusterConfig.getClusterHost()), esClusterConfig.getTransPort()));
            System.out.println("初始化 ES client  successed!");
        } catch (UnknownHostException e) {
            System.out.println("连接 ES 失败，休息一秒后 重新连接...");
            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (Exception e2) {
                System.out.println("连接ES失败，休息一秒时发生异常");
            }
            initEsClient();
        }
    }

    /**
     * 获取连接
     *
     * @return TransportClient
     */
    public static TransportClient getEsClient() {
        if (esClient == null) {
            synchronized (lock) {
                try {
                    initEsClient();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return esClient;
    }

}
	
