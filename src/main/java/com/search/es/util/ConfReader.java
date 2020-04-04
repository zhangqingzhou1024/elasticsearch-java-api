package com.search.es.util;


import com.search.es.bean.ElasticClusterConf;

import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/**
 * 配置文件读取类
 *
 * @author zqz
 * @version 1.0
 * @date 2020-04-04 12:37
 */
public class ConfReader {

    /**
     * 获取 indexMapping path
     *
     * @return indexMapping path
     * @throws Exception
     */
    public static String getIndexMappingPath() {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        URL resource = contextClassLoader.getResource("mapping");
        if (null == resource) {
            throw new NullPointerException("mapping-配置文件路径解析失败！");
        }

        return resource.getPath().replaceAll("%20", " ");
    }


    /**
     * 获取ES集群配置信息
     *
     * @return ES集群配置信息
     * @throws Exception
     */
    public static ElasticClusterConf getESClusterConfig() throws Exception {
        //  读取配置文件
        Properties prop = new Properties();
        InputStream is = ConfReader.class.getClassLoader().getResourceAsStream("config/elasticsearch.properties");
        prop.load(is);
        String clusterHost = prop.getProperty("clusterHost");
        String clusterName = prop.getProperty("clusterName");
        String transPort = prop.getProperty("transPort");

        // 构造配置对象
        return ElasticClusterConf.builder().clusterHost(clusterHost).clusterName(clusterName).transPort(Integer.parseInt(transPort)).build();
    }

}
