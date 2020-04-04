package com.search.es.bean;

import lombok.Builder;
import lombok.Data;

/**
 * ES集群配置信息实体类
 *
 * @author zqz
 * @version 1.0
 * @date 2020-04-04 12:48
 */
@Data
@Builder
public class ElasticClusterConf {
    /**
     * 集群目录
     */
    private String clusterHost;

    /**
     * 集群名称
     */
    private String clusterName;

    /**
     * 端口
     */
    private int transPort;
}
