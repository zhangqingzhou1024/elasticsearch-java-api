package com.search.es;

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
    // 优化成配置文件管理
    private static String clusterName = "liziyuan-log-nodes";
    private static String indexServerStr = "IP";
    private static int transPort = 9300;

    static {
        initEsClient(clusterName, indexServerStr, transPort);
    }

    /**
     * 初始化
     *
     * @param clusterName    集群名称
     * @param indexServerStr 索引地址
     * @param transPort      端口
     */
    private static void initEsClient(String clusterName, String indexServerStr, int transPort) {
        System.out.println("初始化 ES client...");
        try {
            //设置集群名称
            Settings settings = Settings.builder().put("cluster.name", clusterName).build();
            esClient = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(indexServerStr), transPort));
            System.out.println("初始化 ES client  successed!");
        } catch (UnknownHostException e) {
            System.out.println("连接 ES 失败，休息一秒后 重新连接...");
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (Exception e2) {
                System.out.println("连接ES失败，休息一秒时发生异常");
            }
            initEsClient(clusterName, indexServerStr, transPort);
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
                initEsClient(clusterName, indexServerStr, transPort);
            }
        }
        return esClient;
    }


}
	
