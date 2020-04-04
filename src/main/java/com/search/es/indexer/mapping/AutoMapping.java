package com.search.es.indexer.mapping;


import com.search.es.IndexManager;
import com.search.es.util.ConfReader;
import com.thoughtworks.xstream.XStream;
import lombok.extern.log4j.Log4j2;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;


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
     * 索引管理工具类
     */
    private IndexManager indexManager = new IndexManager();
    /**
     * mapping文件存放路径
     */
    private static String mappingPath = ConfReader.getIndexMappingPath();

    public AutoMapping() {
        init();
    }

    /**
     * 初始化解析
     */
    private static void init() {
        System.out.println("mapping config path is " + mappingPath);
        XStream xStream = new XStream();
        xStream.alias("indexConfig", IndexConfig.class);
        try {
            File dir = new File(mappingPath);
            File[] listFiles = dir.listFiles();
            if (null == listFiles) {
                throw new NullPointerException("mapping path dir no files, must be return!");
            }

            System.out.println("mapping-size:" + listFiles.length);
            indexList = new ArrayList<IndexConfig>();
            for (File confFile : listFiles) {
                try {
                    if (confFile.isDirectory() || confFile.getName().contains(".bak")) {
                        continue;
                    }
                    IndexConfig table = (IndexConfig) xStream
                            .fromXML(new FileInputStream(confFile));

                    indexList.add(table);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 进行创建 mapping
     */
    public synchronized void createMapping() {
        if (null == indexList) {
            return;
        }
        for (IndexConfig indexConfig : indexList) {
            indexManager.addMapping(indexConfig);
        }
    }

}
