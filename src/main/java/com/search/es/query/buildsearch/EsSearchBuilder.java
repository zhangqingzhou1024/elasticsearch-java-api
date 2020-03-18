package com.search.es.query.buildsearch;


import com.search.es.query.common.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.range.InternalRange;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.cardinality.InternalCardinality;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.sort.SortOrder;

import java.util.*;
import java.util.concurrent.TimeUnit;


/**
 * ES查询常用方法 封装 类
 *
 * @author zqz
 */
public class EsSearchBuilder {
    private Logger logger = Logger.getLogger(EsSearchBuilder.class);
    /**
     * 是否显示执行时间
     */
    public boolean isShowExecTime = true;
    /**
     * 字段 数据
     */
    private String[] fields;
    /**
     * 索引值
     */
    private String[] indices;
    /**
     * 别名值
     */
    private String[] aliasNames;
    /**
     * 类型 数组
     */
    private String[] types;
    /**
     * 客户端
     */
    private Client client;
    /**
     * 查询器-创建者
     */
    private SearchRequestBuilder searchbuilder;

    /**
     * 结果值
     */
    private SearchResponse response;

    /**
     * 结果值 List,用于 scroll 查询时
     */
    private List<SearchResponse> responseList;
    /**
     * 查询条件组建者
     * 组合查询（布尔查询）各个条件通过 与或非 进行拼接
     * MUST MUST_NOT SHOULD
     * BoolQueryBuilder
     */
    private BoolQueryBuilder query;
    /**
     * 查询条件-组合 token方式
     */
    private QueryStringQueryBuilder queryStringBuilder;

    /**
     * 起始索引
     */
    private int start = 0;
    /**
     * 设置 返回结果的 size
     * 初始化值 -1
     */
    private int rows = 10;

    /**
     * scroll 方式 每批次 取得的 数据
     */
    private int scrollBatchNum = 1000;

    /**
     * 是否为 scroll 方式执行
     */
    private boolean isExecuteByScroll = false;
    /**
     * facetName
     */
    private final String facetName = "elastic-facet";
    /**
     * 聚合字段后缀
     */
    private String[] aggName;

    /**
     * 度量聚合 存储容器
     */
    private MetricAgg metricAgg;
    /**
     * 获取 source 标志位
     */
    private boolean getSourceFlag;

    /**
     * 高亮查询拆分后 数组
     */
    private String[] hightLightContentArr;

    /**
     * @param index 索引名称--相当于 mysql 中 数据库
     * @param type  类型---->相当于 mysql 中 table  单 类型
     *              集群信息 读取配置文件
     */
    public EsSearchBuilder(String index, String type) {
        String[] indices = {index};
        String[] types = {type};
        if (type == null) {
            types = null;
        }
        if (index == null) {
            indices = null;
        }
        setInitQueryCnd(indices, types);
    }

    public EsSearchBuilder() {
        setInitQueryCnd(null, null);
    }

    /**
     * 暂不开放  别名
     *
     * @param alisaNames 别名
     */
    private void EsSearchClientInAlias(String[] alisaNames) {
        setInitQueryCndByAliasName(alisaNames);
    }

    /**
     * @param indexName 索引名称--相当于 mysql 中 数据库
     *                  集群信息 读取配置文件
     */
    public EsSearchBuilder(String indexName) {
        String[] indices = {indexName};
        if (indexName == null) {
            indices = null;
        }
        setInitQueryCnd(indices, null);
    }

    /**
     * @param indices 索引名称--相当于 mysql 中 数据库
     * @param types   类型---->相当于 mysql 中 table  多类型
     */
    public EsSearchBuilder(String[] indices, String[] types) {
        setInitQueryCnd(indices, types);
    }

    /**
     * @param indices 索引名称--相当于 mysql 中 数据库
     */
    public EsSearchBuilder(String[] indices) {
        setInitQueryCnd(indices, null);
    }


    /**
     * 设置查询的索引名和类型名
     *
     * @param indices 索引名称--相当于 mysql 中 数据库
     * @param types   类型---->相当于 mysql 中 table
     */
    private void setInitQueryCnd(String[] indices, String[] types) {
        this.indices = indices;
        this.types = types;

        try {
            //设置集群名称
            this.client = TransportClientFactory.getEsClient();
            initResponse();
        } catch (Exception e) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e2) {
                logger.error(e2.getMessage());
            }
            // 重试机制
            setInitQueryCnd(indices, types);
        }
    }

    /**
     * 初始化 es-java client
     * 暂不开发 别名
     */
    private void setInitQueryCndByAliasName(String[] aliasNames) {
        this.aliasNames = aliasNames;
        try {
            //设置集群名称
            this.client = TransportClientFactory.getEsClient();
            initResponse();
        } catch (Exception e) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e2) {
                logger.error(e2.getMessage());
            }
            setInitQueryCndByAliasName(aliasNames);
        }
    }

    /**
     * 索引 数组
     *
     * @param indices
     */
    private void initCoreServer(String[] indices) {
        setInitQueryCnd(indices, null);
    }

    /**
     * 初始化结果集
     */
    private void initResponse() {
        this.fields = null;
        this.query = new BoolQueryBuilder();
        this.responseList = new ArrayList<SearchResponse>();
        this.queryStringBuilder = null;
        this.getSourceFlag = false;
        // 没有别名
        if (this.aliasNames == null || this.aliasNames.length == 0) {
            if ((this.indices == null) || (this.indices.length == 0)) {
                this.searchbuilder = this.client.prepareSearch(new String[0]);
            } else {
                this.searchbuilder = this.client.prepareSearch(this.indices);
            }
            if ((this.types != null) && (this.types.length > 0)) {
                this.searchbuilder = this.searchbuilder.setTypes(this.types);
            }
        } else {
            if (this.aliasNames != null && this.aliasNames.length != 0) {
                this.searchbuilder = this.client.prepareSearch(this.aliasNames);
            } else {
                throw new NullPointerException("elastic index-aliasNames must be not null");
            }
        }
    }

    /**
     * 多次执行时，应在执行之前 重设一下
     * 清空 上次 记录
     */
    public void reset() {
        initResponse();
        this.start = 0;
        this.rows = 10;
        this.aggName = null;
        this.fields = null;
        this.isExecuteByScroll = false;
        isShowExecTime = true;
        this.hightLightContentArr = null;
    }

    /**
     * 增加查询条件
     * 检索方式介绍：
     * 1.先把 keywords  按照检索的分词器 分成多个 token
     * <p>
     * 2.AND ： 要检索的 tokens必须在  field中 全部匹配才能返回doc
     * OR： 要检索的 tokens只要在  field中 匹配出一个就能返回doc
     * <p>
     * OR 是 比 AND 范围更广，但精确度 相对较低的 方式
     *
     * @param field     列名
     * @param keywords  查询值
     * @param queryType 表示 与其它 查询条件 的关系
     * @param combine   拼接方式 and/or链式条件 拼装方式
     *                  <p>
     *                  http://www.tuicool.com/articles/qqymmaJ  多字段匹配
     */
    public void addKeywordsQuery(String field, String keywords, QueryTypeEnum queryType, KeywordsCombineEnum combine) {
        if ((!(checkStr(field))) || (!(checkStr(keywords)))) {
            return;
        }
        // 由关键词操作方式  -->匹配出查询方式
        Operator ope = null;
        if (combine.equals(KeywordsCombineEnum.AND)) {
            ope = Operator.AND;
        } else {
            ope = Operator.OR;
        }
        MatchQueryBuilder matchBuilder = QueryBuilders.matchQuery(field, keywords).operator(ope);
        addQuery(matchBuilder, queryType);
    }

    /**
     * 增加查询条件
     * 检索方式介绍：
     * 1.先把 keywords  按照检索的分词器 分成多个 token
     * <p>
     * 2.AND ： 要检索的 tokens必须在  fields【字段集】中 全部匹配才能返回doc
     * OR： 要检索的 tokens只要在  fields【字段集】中 匹配出一个就能返回doc
     * <p>
     * OR 是 比 AND 范围更广，但精确度 相对较低的 方式
     *
     * @param fields    列名 -- 多列
     * @param keywords  查询值
     * @param queryType 表示 与其它 查询条件 的关系
     * @param combine   拼接方式 and/or链式条件 拼装方式
     *                  <p>
     *                  http://www.tuicool.com/articles/qqymmaJ  多字段匹配
     */
    public void addKeywordsQuery(String[] fields, String keywords, QueryTypeEnum queryType, KeywordsCombineEnum combine) {
        if ((fields == null) || (fields.length == 0) || (!(checkStr(keywords)))) {
            return;
        }
        // 由关键词操作方式  -->匹配出查询方式
        Operator ope = null;
        if (combine.equals(KeywordsCombineEnum.AND)) {
            ope = Operator.AND;
        } else {
            ope = Operator.OR;
        }

        MultiMatchQueryBuilder match = QueryBuilders.multiMatchQuery(keywords, fields).operator(ope);
        addQuery(match, queryType);
    }

    /**
     * 规则解析 查询
     *
     * @param field
     * @param rules 规则
     *              如：
     *              (南宁|桂林)&(反腐&贪污|打劫)!(楼盘|广告)
     *              免疫球蛋白&(艾滋病|HIV)
     *              宁夏&(清真|伊斯兰|宗教|民族|穆斯林)
     * @param field
     * @param rules
     */
    public void addKeywordsRuleQuery(String field, String rules) {
        if (StringUtils.isEmpty(rules)) {
            setNoResultQuery();
            return;
        }
        String splitStr = " ";
        KeywordsFormat keywordsFormat = new KeywordsFormat();
        rules = rules.replaceAll("" + splitStr + "+", splitStr);
        rules = rules.replaceAll(" ", "&");
        List<String[]> keywordsList = keywordsFormat.exchangeKws(rules);
        if (null == keywordsList && keywordsList.size() == 0) {
            setNoResultQuery();
            return;
        }
        this.addKeywordsRuleQuery(field, keywordsList, QueryTypeEnum.MUST);
    }

    /**
     * 设置 空条件查询
     */
    public void setNoResultQuery() {
        System.out.println("关键词规则解析失败，请检查是否符合规则，当前设置空查询!!!");
        addPrimitiveTermQuery("_id", "-1", QueryTypeEnum.MUST);
    }

    /**
     * @param field
     * @param keywordsList List<String[]>
     *                     字符数组 长度为2
     *                     0:have包含关键词  1:noHave 不包含关键词
     * @param queryType
     */
    public void addKeywordsRuleQuery(String field, List<String[]> keywordsList, QueryTypeEnum queryType) {
        if (null == keywordsList && keywordsList.size() == 0) {
            return;
        }
        if (keywordsList == null || keywordsList.size() == 0) {
            return;
        }
        BoolQueryBuilder bool = new BoolQueryBuilder();
        BoolQueryBuilder subBool = null;

        MatchPhraseQueryBuilder h = null;
        MatchPhraseQueryBuilder n = null;
        String mustNotHave = "";
        for (String[] strings : keywordsList) {
            subBool = new BoolQueryBuilder();
            String have = strings[0];
            String no = strings[1];

            // 包含
            String[] split = have.split(" ");
            if (split != null && split.length > 0) {
                for (String must : split) {
                    h = QueryBuilders.matchPhraseQuery(field, must);
                    subBool.must(h);
                }
            }
            if (no != null && no.trim().length() > 0) {
                mustNotHave = no;
            }
            bool.should(subBool);
        }
        //包含关键词
        this.addQuery(bool, queryType);
        // 不包含关键词
        if (mustNotHave != null && mustNotHave.trim().length() > 0) {
            BoolQueryBuilder noBool = null;
            //bbb|楼盘 广告|aaa
            String[] split = mustNotHave.split("\\|");
            // 或的关系
            if (split.length > 0) {
                noBool = new BoolQueryBuilder();
                for (String nohave : split) {

                    nohave = nohave.trim();
                    String phrase = null;
                    BoolQueryBuilder subNoBool = null;
                    if (nohave.contains(" ")) {
                        subNoBool = new BoolQueryBuilder();
                        String[] split02 = nohave.split(" ");
                        for (String s : split02) {
                            phrase = s;
                            MatchPhrasePrefixQueryBuilder match = QueryBuilders.matchPhrasePrefixQuery(field, phrase).slop(0);

                            subNoBool.must(match);
                        }
                        noBool.should(subNoBool);
                    } else {
                        n = QueryBuilders.matchPhraseQuery(field, nohave);
                        noBool.should(n);
                    }

                }
            }

            this.addQuery(noBool, QueryTypeEnum.MUST_NOT);
        }

    }

    /**
     * 多字段(多组)关键词查询
     *
     * @param conditionList List<KeywordsQueryBean> 条件拼接实体
     * @param queryType
     */
    public void addKeywordsQuery(List<KeywordsCondition> conditionList, QueryTypeEnum queryType) {
        if (conditionList == null || conditionList.size() == 0) {
            return;
        }
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        MatchQueryBuilder matchBuilder = null;
        String keywords = null;
        Operator ope = null;
        for (KeywordsCondition queryBean : conditionList) {
            if (queryBean.getField() != null && queryBean.getKeywords() != null && !queryBean.equals("")) {
                if (queryBean.getCombine().equals(KeywordsCombineEnum.AND)) {
                    ope = Operator.AND;
                } else {
                    ope = Operator.OR;
                }
                matchBuilder = QueryBuilders.matchQuery(queryBean.getField(), keywords).operator(ope);

                if (queryBean.getOperator().equals(QueryTypeEnum.MUST)) {
                    boolQuery.must(matchBuilder);
                } else if (queryBean.getOperator().equals(QueryTypeEnum.MUST_NOT)) {
                    boolQuery.mustNot(matchBuilder);
                } else {
                    boolQuery.should(matchBuilder);
                }
            }
        }
        addQuery(boolQuery, queryType);

    }

    /**
     * 短语查询
     * 查询方式 和KeywordsQuery 有点类似
     * 查询方式：
     * 第一步：短语形式查询，查询时关键字不会被分词，而是直接以一个字符串的形式查询
     * 第二步：然后把 tokens 与 fields-value 字段值 分成的 tokens 进行匹配
     * 当值 、顺序位置 都匹配上时 返回文档，
     *
     * @param field
     * @param phrase
     * @param queryType 相关文档
     *                 http://www.blogjava.net/persister/archive/2009/07/14/286634.html
     */
    public void addPhraseQuery(String field, String phrase, QueryTypeEnum queryType) {
        if ((!(checkStr(field))) || (!(checkStr(phrase)))) {
            return;
        }
        MatchPhraseQueryBuilder match = QueryBuilders.matchPhraseQuery(field, phrase);
        //	QueryBuilders.m
        addQuery(match, queryType);
    }

    /**
     * 短语查询
     * 查询方式 和KeywordsQuery 有点类似
     * 查询方式：
     * 第一步：先把 keywords 查询值 按照分词器 分成 tokens
     * 第二步：然后把 tokens 与 fields-value 字段值 分成的 tokens 进行匹配
     * 当值 、顺序位置 都匹配上时 返回文档，
     *
     * @param field
     * @param phrase
     * @param queryType 相关文档
     *                 http://www.blogjava.net/persister/archive/2009/07/14/286634.html
     */
    public void addPhraseQuery(String field, String phrase, int slop, QueryTypeEnum queryType) {
        if ((!(checkStr(field))) || (!(checkStr(phrase)))) {
            return;
        }
        MatchPhraseQueryBuilder match = QueryBuilders.matchPhraseQuery(field, phrase).slop(slop);
        //	QueryBuilders.m
        addQuery(match, queryType);
    }

    /**
     * 短语查询 -- 增加位置的容忍度
     *
     * @param field
     * @param phrase
     * @param slop      移动次数（位置偏移量-相对于token）
     * @param queryType 查询方式：
     *                  第一步：短语形式查询，查询时关键字不会被分词，而是直接以一个字符串的形式查询
     *                  第二步：然后把 tokens 与 fields-value 字段值 分成的 tokens 进行匹配
     *                  当没有 slop (对位置容忍度放大)时，tokens 必须一致 才会返回文档
     *                  当值 、顺序位置 都匹配上时 返回文档，
     *                  eg：
     *                  首先，强调一下PhraseQuery对象，这个对象不属于跨度查询类，但能完成跨度查询功能。
     *                  匹配到的文档所包含的项通常是彼此相邻的，考虑到原文档中在查询项之间可能有一些中间项，
     *                  或为了能查询倒排的项，PhraseQuery设置了slop因子，但是这个slop因子指2个项允许最大间隔距离，
     *                  不是传统意义上的距离，是按顺序组成给定的短语，所需要移动位置的次数，
     *                  这表示PhraseQuery是必须按照项在文档中出现的顺序计算跨度的，如quick brown fox为文档，
     *                  则quick fox2个项的slop为1，quick向后移动一次.而fox quick需要quick向后移动3次，所以slop为3
     */
    public void addPhrasePrefixQuery(String field, String phrase, int slop, QueryTypeEnum queryType) {
        if ((!(checkStr(field))) || (!(checkStr(phrase)))) {
            return;
        }
        MatchPhrasePrefixQueryBuilder match = QueryBuilders.matchPhrasePrefixQuery(field, phrase).slop(slop);
        addQuery(match, queryType);
    }

    /**
     * 单字段 多值匹配
     *
     * @param field      字段名
     * @param contextStr 要匹配的字符串
     * @param splitStr   拆分标志
     * @param slop       跨度
     * @param queryType  操作方式
     */
    public void addPhrasePrefixQuery(String field, String contextStr, String splitStr, int slop, QueryTypeEnum queryType) {
        // 内容 匹配 查询 高亮
        String[] lightStr = null;
        if (contextStr != null && !contextStr.equals("")) {
            contextStr = contextStr.replaceAll("" + splitStr + "+", splitStr);
            if (contextStr.trim().lastIndexOf(splitStr) == -1) {
                lightStr = new String[1];
                lightStr[0] = contextStr;
                addPhrasePrefixQuery(field, contextStr, slop, queryType);
            } else {
                String[] split = contextStr.split(" ");
                lightStr = new String[split.length];
                for (int i = 0; i < split.length; i++) {
                    String str = split[i];
                    lightStr[i] = split[i];
                    addPhrasePrefixQuery(field, str, slop, queryType);
                }
            }
        }
        if (lightStr != null) {
            if (this.hightLightContentArr != null && this.hightLightContentArr.length > 0) {

                String[] tempArr = new String[this.hightLightContentArr.length + lightStr.length];
                int index = 0;
                for (String str : this.hightLightContentArr) {
                    if (index < tempArr.length) {
                        tempArr[index++] = str;
                    }
                }
                for (String str : lightStr) {
                    if (index < tempArr.length) {
                        tempArr[index++] = str;
                    }
                }
                this.hightLightContentArr = tempArr;
            } else {
                this.hightLightContentArr = lightStr;

            }
        }
    }

    /**
     * 多字段 多值匹配
     * 只要满足一个字段即可
     *
     * @param field      字段名
     * @param contextStr 要匹配的字符串
     * @param operator   操作方式
     */
    public void addMultFieldsPhrasePrefixQuery(String[] fields, String contextStr, QueryTypeEnum queryType) {
        // 内容 匹配 查询 高亮
        String splitStr = " ";
        String phrase = null;
        if (fields == null || fields.length == 0) {
            return;
        }
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        for (String field : fields) {
            BoolQueryBuilder fieldQuery = QueryBuilders.boolQuery();

            if (contextStr != null && !contextStr.equals("")) {
                contextStr = contextStr.replaceAll("" + splitStr + "+", splitStr);
                if (contextStr.trim().lastIndexOf(splitStr) == -1) {

                    MatchPhrasePrefixQueryBuilder match = QueryBuilders.matchPhrasePrefixQuery(field, phrase).slop(0);
                    fieldQuery.must(match);
                } else {
                    String[] split = contextStr.split(" ");
                    for (int i = 0; i < split.length; i++) {
                        MatchPhrasePrefixQueryBuilder match = QueryBuilders.matchPhrasePrefixQuery(field, phrase).slop(0);

                        fieldQuery.must(match);
                    }
                }
            }

            boolQuery.should(fieldQuery);
        }
        addQuery(boolQuery, queryType);
    }

    private void addMultFieldsPhrasePrefixQueryBySeg(String[] fields, String contextStr, QueryTypeEnum queryType) {
        // 内容 匹配 查询 高亮
        String phrase = null;
        if (fields == null || fields.length == 0) {
            return;
        }
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        for (String field : fields) {
            BoolQueryBuilder fieldQuery = QueryBuilders.boolQuery();

            if (!StringUtils.isEmpty(contextStr)) {
                MatchPhrasePrefixQueryBuilder match = QueryBuilders.matchPhrasePrefixQuery(field, phrase).slop(0);
                fieldQuery.must(match);
                //}
            }

            boolQuery.should(fieldQuery);
        }
        addQuery(boolQuery, queryType);
    }

    /**
     * 最原始的 term 查询方法
     * <p>
     * 匹配方式  和 分词词有关（token）
     * 不分词-> 整体为一个 token
     * 把查询条件term 当成一个 token-1,然后去与filed value 拆分的tokens 去匹配，若
     * 在tokens中 存在 token-1 则返回该文档
     *
     * @param field
     * @param term      值
     * @param queryType 表示 与其它 查询条件 的关系
     */
    public void addPrimitiveTermQuery(String field, Object term, QueryTypeEnum queryType) {
        if ((!(checkStr(field))) || (!(checkStr(term)))) {
            return;
        }
        addQuery(QueryBuilders.termQuery(field, term.toString().trim()), queryType);
    }

    /**
     * 多 term匹配是对QueryStringQueryBuilder 的进行的封装
     * 匹配步骤：
     * 第一步：把多个terms 和在一起 然后进行 分词拆分->tokens
     * 第二步：去 field-value 中的 tokens 库中去匹配，
     * 只要有一个匹配上 就返回回档
     *
     * @param field     字段
     * @param terms     匹配值数组
     * @param queryType 与其他查询条件的关系
     */
    public void addPrimitiveTermQuery(String field, String[] terms, QueryTypeEnum queryType) {
        if ((!(checkStr(field))) || (terms == null) || (terms.length == 0)) {
            return;
        }
        String term = "";
        for (int i = 0; i < terms.length; ++i) {
            String termIt = terms[i];
            term = term + termIt;
            if (i < terms.length - 1) {
                term = term + " ";
            }
        }
        String queryString = term;
        QueryStringQueryBuilder query = new QueryStringQueryBuilder(queryString);
        query.defaultOperator(QueryStringQueryBuilder.DEFAULT_OPERATOR);
        query.defaultField(field);

        addQuery(query, queryType);
    }

    /**
     * 多 term匹配是对QueryStringQueryBuilder 的进行的封装
     * 匹配步骤：
     * 第一步：把多个terms 和在一起 然后进行 分词拆分->tokens
     * 第二步：去 field-value 中的 tokens 库中去匹配，
     * 只要有一个匹配上 就返回回档
     *
     * @param field     字段
     * @param termList  匹配值数组
     * @param queryType 与其他查询条件的关系
     */
    public void addPrimitiveTermQuery(String field, List<String> termList, QueryTypeEnum queryType) {
        if ((!(checkStr(field))) || (termList == null) || (termList.size() == 0)) {
            return;
        }
        // list  to array
        String[] termArr = new String[termList.size()];
        termArr = termList.toArray(termArr);

        addPrimitiveTermQuery(field, termArr, queryType);
    }

    /**
     * 支持java 正则表达式
     * 词条（token）级别
     *
     * @param field     字段
     * @param regexpStr 正则表达式
     * @param queryType 与其他查询条件的拼接关系
     *                  prefix，wildcard以及regexp查询基于词条(token)进行操作
     *                  http://blog.csdn.net/dm_vincent/article/details/42024799
     */
    public void addRegexpQuery(String field, String regexpStr, QueryTypeEnum queryType) {
        if ((!(checkStr(field))) || (!(checkStr(regexpStr)))) {
            return;
        }
        RegexpQueryBuilder regex = new RegexpQueryBuilder(field, regexpStr);
        addQuery(regex, queryType);
    }

    /**
     * token 前缀 查询
     * 这个地方的Prefix 是对 filed-value 分词后tokens 所说的
     * 查询步骤：
     * 第一步：整个 term 值会充当一个token的前缀（一个整体 不再分词了）】
     * 第二步：拿到 上面的 值 取 文档库中 匹配 filed -value 中 所有token ,只要满足库中token前缀 与 term 一致时
     * 返回文档
     *
     * @param field     字段
     * @param term      （token）的前缀值
     * @param queryType 与之前查询条件的 拼接关系
     */
    public void addPrefixQuery(String field, String term, QueryTypeEnum queryType) {
        if ((!(checkStr(field))) || (!(checkStr(term)))) {
            return;
        }
        PrefixQueryBuilder prefix = new PrefixQueryBuilder(field, term);
        addQuery(prefix, queryType);
    }

    /**
     * 通配符查询
     * 基于 token(词条)
     * eg；"W?FHW
     * 可以把 WkEHW 的查出来
     *
     * @param field
     * @param term
     * @param queryType http://blog.csdn.net/dm_vincent/article/details/42024799
     */
    public void addWildcardQuery(String field, String term, QueryTypeEnum queryType) {
        if ((!(checkStr(field))) || (!(checkStr(term)))) {
            return;
        }

        WildcardQueryBuilder wildQuery = new WildcardQueryBuilder(field, term);
        addQuery(wildQuery, queryType);
    }

    /**
     * 模糊查询
     * 支持的不是很好
     *
     * @param field
     * @param term
     * @param queryType
     */
    @Deprecated
    private void addFuzzyQuery(String field, String term, QueryTypeEnum queryType) {
        if ((!(checkStr(field))) || (!(checkStr(term)))) {
            return;
        }
        FuzzyQueryBuilder fuzzyQueryBuilder = new FuzzyQueryBuilder(field, term);
        //WildcardQueryBuilder wildQuery = new WildcardQueryBuilder(field, term);
        addQuery(fuzzyQueryBuilder, queryType);
    }

    /**
     * 增加值域范围查询
     *
     * @param field        字段
     * @param startTerm    起始值  无限制 可为 null
     * @param startOperate 规定> or >=
     * @param endTerm      结束值  无限制 可为 null
     * @param endOperate   规定< or <=
     * @param queryType    与其他查询条件的关系
     *                     <p>
     *                     如果是时间范围查询，字符格式一定要与 mapping中 格式 一致
     */
    public void addRangeQuery(String field, Object startTerm, RangeCommon startOperate, Object endTerm,
                              RangeCommon endOperate, QueryTypeEnum queryType) {
        if ((!(checkStr(field))) || ((!(checkStr(startTerm))) && (!(checkStr(endTerm))))) {
            return;
        }
        RangeQueryBuilder builder = new RangeQueryBuilder(field);
        if (startTerm != null) {
            if (startOperate == RangeCommon.GT) {
                builder = builder.gt(startTerm);
            } else if (startOperate == RangeCommon.GTE) {
                builder = builder.gte(startTerm);
            }
        }
        if (endTerm != null) {
            if (endOperate == RangeCommon.LT) {
                builder = builder.lt(endTerm);
            } else if (endOperate == RangeCommon.LTE) {
                builder = builder.lte(endTerm);
            }
        }
        addQuery(builder, queryType);
    }

    /**
     * 自定义 QueryBuilder
     * 拼接到 已有的 query
     *
     * @param query
     * @param queryType
     */
    public void addQuery(QueryBuilder query, QueryTypeEnum queryType) {
        if (queryType.equals(QueryTypeEnum.MUST)) {
            this.query.must(query);
        } else if (queryType.equals(QueryTypeEnum.MUST_NOT)) {
            this.query.mustNot(query);
        } else {
            this.query.should(query);
        }
    }


    /**
     * 支持 lucene 字符串 查询<br>
     * 与其它查询条件互斥，设置此种条件时 其他条件不执行
     *
     * @param queryString
     */
    public void addQueryString(String queryString) {
        this.queryStringBuilder = new QueryStringQueryBuilder(queryString);
    }


    /**
     * 范围聚合即给聚合添加 限定条件
     *
     * @param field      聚合字段
     * @param rangesList 每一个元素 都是 统一级别，不是sub 方式追加
     */
    private void addRangeFacet(String field, List<RangeTerms> rangesList) {
        if ((field == null) || (rangesList == null) || (rangesList.size() == 0)) {
            return;
        }
        this.aggName = new String[1];
        this.aggName[0] = this.facetName;
        RangeAggregationBuilder builder = AggregationBuilders.range(this.aggName[0]).field(field);
        for (RangeTerms range : rangesList) {
            if ((range.getFrom() > -1.0D) && (range.getTo() > -1.0D)) {
                builder = builder.addRange(range.getTermName(), range.getFrom(), range.getTo());
            } else if ((range.getFrom() == -1.0D) && (range.getTo() > -1.0D)) {
                builder = builder.addUnboundedTo(range.getTermName(), range.getTo());
            } else if ((range.getFrom() > -1.0D) && (range.getTo() == -1.0D)) {
                builder = builder.addUnboundedFrom(range.getTermName(), range.getFrom());
            }
        }
        this.searchbuilder.addAggregation(builder);
    }

    /**
     * 此方法 中 不能与其它聚合 共存
     * 多字段 聚合（Aggregation）统计  桶聚合
     * 相当于 关系型 数据库中的 group by
     * 按字段分组进行统计，当聚合的字段 不止一个时，采用链式添加的方式 深层次 统计
     * 比如：一个班级 同学 有男有女 有姓张的 有姓李的，
     * fields[0]="sex";表示性别
     * fields[1]="family_name";表示姓
     * 这是 我们 可以统计出 这个班级里面
     * 男生中姓张的同学个数等
     *
     * @param fields       聚合时 应设置 rows = 0 ,减少io,当rows > 10000 时 不能聚合
     * @param topN         此TopN 为 每个分片 的topN
     * @param orderByCount true： docCount 排序
     * @param isDesc       true: 降序  false :升序
     */
    public void addMultFieldsStat(String[] fields, int topN, boolean orderByCount, boolean isDesc) {
        if ((fields == null) || (fields.length == 0)) {
            return;
        }
        this.aggName = new String[fields.length];
        TermsAggregationBuilder termsBuilder = null;
        int i = 0;
        TermsAggregationBuilder topBuilder = null;
        do {
            this.aggName[i] = "agg_" + fields[i];
            // head
            if (i == 0) {
                termsBuilder = AggregationBuilders.terms("agg_" + fields[i]).field(fields[i]).size(topN);
                // 排序
                if (orderByCount) {
                    termsBuilder = termsBuilder.order(Terms.Order.count(!(isDesc)));
                } else {
                    termsBuilder = termsBuilder.order(Terms.Order.term(!(isDesc)));
                }// tail
            } else if (i == fields.length - 1) {
                TermsAggregationBuilder subTerm = AggregationBuilders.terms("agg_" + fields[i]).field(fields[i])
                        .size(topN);
                if (orderByCount) {
                    subTerm = subTerm.order(Terms.Order.count(!(isDesc)));
                } else {
                    subTerm = subTerm.order(Terms.Order.term(!(isDesc)));
                }
                if (topBuilder == null) {
                    topBuilder = subTerm;
                    termsBuilder = termsBuilder.subAggregation(subTerm).size(topN);
                } else {
                    topBuilder = topBuilder.subAggregation(subTerm);
                }
                // middle
            } else {
                TermsAggregationBuilder builder = AggregationBuilders.terms("agg_" + fields[i]).field(fields[i]).size(topN);
                // 排序
                if (orderByCount) {
                    builder = builder.order(Terms.Order.count(!(isDesc)));
                } else {
                    builder = builder.order(Terms.Order.term(!(isDesc)));
                }
                if (topBuilder == null) {
                    topBuilder = builder;
                    termsBuilder = termsBuilder.subAggregation(builder).size(topN);
                } else {
                    topBuilder = topBuilder.subAggregation(builder).size(topN);
                }
            }
            ++i;
        } while (i < fields.length);

        this.searchbuilder.addAggregation(termsBuilder);
    }

    /**
     * 此方法 中 不能与其它聚合 共存
     * 多字段 聚合（Aggregation）统计  桶聚合
     * 相当于 关系型 数据库中的 group by
     * 按字段分组进行统计，当聚合的字段 不止一个时，采用链式添加的方式 深层次 统计
     * 比如：一个班级 同学 有男有女 有姓张的 有姓李的，
     * fields[0]="sex";表示性别
     * fields[1]="family_name";表示姓
     * 这是 我们 可以统计出 这个班级里面
     * 男生中姓张的同学个数等
     *
     * @param fields             聚合时 应设置 rows = 0 ,减少io,当rows > 10000 时 不能聚合
     * @param topN               此TopN 为 每个分片 的topN
     * @param orderByCount       true： docCount 排序
     * @param isDesc             true: 降序  false :升序
     * @param minDocCounts       各分片汇总后最低的count
     * @param shardsMinDocCounts 每个分片汇总后最低的count
     */
    public void addMultFieldsStat(String[] fields, int topN, boolean orderByCount, boolean isDesc, Long[] minDocCounts, Long[] shardsMinDocCounts) {
        if ((fields == null) || (fields.length == 0) || minDocCounts == null || minDocCounts.length == 0 || shardsMinDocCounts == null || shardsMinDocCounts.length == 0) {
            return;
        }
        this.aggName = new String[fields.length];
        TermsAggregationBuilder termsBuilder = null;
        int i = 0;
        TermsAggregationBuilder topBuilder = null;
        do {
            this.aggName[i] = "agg_" + fields[i];
            // head
            if (i == 0) {
                termsBuilder = AggregationBuilders.terms("agg_" + fields[i]).field(fields[i]).size(topN)
                        .minDocCount(minDocCounts[i]).shardMinDocCount(shardsMinDocCounts[i]);
                // 排序
                if (orderByCount) {
                    termsBuilder = termsBuilder.order(Terms.Order.count(!(isDesc)));
                } else {
                    termsBuilder = termsBuilder.order(Terms.Order.term(!(isDesc)));
                }// tail
            } else if (i == fields.length - 1) {
                TermsAggregationBuilder subTerm = AggregationBuilders.terms("agg_" + fields[i]).field(fields[i])
                        .size(topN).minDocCount(minDocCounts[i]).shardMinDocCount(shardsMinDocCounts[i]);
                if (orderByCount) {
                    subTerm = subTerm.order(Terms.Order.count(!(isDesc)));
                } else {
                    subTerm = subTerm.order(Terms.Order.term(!(isDesc)));
                }
                if (topBuilder == null) {
                    topBuilder = subTerm;
                    termsBuilder = termsBuilder.subAggregation(subTerm).size(topN);
                } else {
                    topBuilder = topBuilder.subAggregation(subTerm);
                }
                // middle
            } else {
                TermsAggregationBuilder builder = AggregationBuilders.terms("agg_" + fields[i]).field(fields[i]).size(topN)
                        .minDocCount(minDocCounts[i]).shardMinDocCount(shardsMinDocCounts[i]);
                // 排序
                if (orderByCount)
                    builder = builder.order(Terms.Order.count(!(isDesc)));
                else {
                    builder = builder.order(Terms.Order.term(!(isDesc)));
                }
                if (topBuilder == null) {
                    topBuilder = builder;
                    termsBuilder = termsBuilder.subAggregation(builder).size(topN);
                } else {
                    topBuilder = topBuilder.subAggregation(builder).size(topN);
                }
            }
            ++i;
        } while (i < fields.length);

        this.searchbuilder.addAggregation(termsBuilder);
    }

    /**
     * 这是一个 二级聚合，即 先按statField进行聚合，在其内部 在按照dateField字段 进行日期聚合
     * 某字段statField
     *
     * @param statField
     * @param dateField 时间分组统计字段
     * @param dateType  0：day 1: week   2：hour
     * @param interval  时间间隔，比如 按一天 聚合则跨度为1，interval =1 ，跨度为2  则赋值为 2
     */
    public void addFieldStatByDate(String statField, String dateField, Integer dateType, Integer interval) {
        if ((!(checkStr(statField))) || (!(checkStr(dateField)))) {
            return;
        }
        this.aggName = new String[2];
        this.aggName[0] = "agg_" + statField;
        this.aggName[1] = "agg_" + dateField;
        TermsAggregationBuilder firstAgg = AggregationBuilders.terms("agg_" + statField).field(statField);
        DateHistogramAggregationBuilder dateAgg = AggregationBuilders.dateHistogram("agg_" + dateField).field(dateField);
        DateHistogramInterval interType = null;
        interType = DateHistogramInterval.days(1);
        dateAgg.format("yyyy-MM-dd HH:mm:ss");
        if (dateType != null) {
            if (interval != null) {
                if (dateType == 0) {
                    interType = DateHistogramInterval.days(interval);
                } else if (dateType == 1) {
                    interType = DateHistogramInterval.weeks(interval);
                } else if (dateType == 2) {
                    interType = DateHistogramInterval.hours(interval);
                }
            } else {
                if (dateType == 1) {
                    interType = DateHistogramInterval.weeks(1);
                } else if (dateType == 2) {
                    interType = DateHistogramInterval.hours(1);
                }
            }
        }
        dateAgg.dateHistogramInterval(interType);
        // 结构化
        firstAgg.subAggregation(dateAgg);
        this.searchbuilder.addAggregation(firstAgg);
    }

    /**
     * 按时间 进行分组
     *
     * @param dateField
     * @param dateType  0：day 1: week   2：hour
     */
    public void addStatByDate(String dateField, Integer dateType, Integer interval) {
        if (!(checkStr(dateField))) {
            return;
        }
        this.aggName = new String[1];
        this.aggName[0] = "agg_" + dateField;
        DateHistogramAggregationBuilder dateAgg = AggregationBuilders.dateHistogram("agg_" + dateField).field(dateField);
        DateHistogramInterval interType = null;
        dateAgg.format("yyyy-MM-dd HH:mm:ss");
        interType = DateHistogramInterval.days(1);
        if (dateType != null) {
            if (interval != null) {
                if (dateType == 1) {
                    interType = DateHistogramInterval.weeks(interval);
                } else if (dateType == 2) {
                    interType = DateHistogramInterval.hours(interval);
                }
            } else {
                if (dateType == 1) {
                    interType = DateHistogramInterval.weeks(1);
                } else if (dateType == 2) {
                    interType = DateHistogramInterval.hours(1);
                }
            }
        }
        dateAgg.dateHistogramInterval(interType);
        this.searchbuilder.addAggregation(dateAgg);
    }

    /**
     * Elasticsearch 目前支持两种近似算法（cardinality（基数） 和 percentiles（百分位数））。
     * 它们会提供准确但不是 100% 精确的结果。
     * 以牺牲一点小小的估算错误为代价，这些算法可以为我们换来高速的执行效率和极小的内存消耗。
     * https://www.cnblogs.com/richaaaard/p/5319299.html
     *
     * @param field              聚合字段
     * @param precisionThreshold 精确度百分比, 此值越大 精确度越低
     */
    public void addCardinalityAgg(String field, long precisionThreshold) {
        if (field == null || field.equals("")) {
            return;
        }
        AggregationBuilder cardinalityAgg = AggregationBuilders.cardinality("agg_" + field).field(field)
                .precisionThreshold(precisionThreshold);
        this.aggName = new String[1];
        this.aggName[0] = "agg_" + field;

        this.searchbuilder.addAggregation(cardinalityAgg);
    }

    /**
     * 度量聚合
     * 与其它聚合方式 互斥  覆盖现象
     *
     * @param aggList
     */
    public void addMetricAgg(String field, MetricEnum metric) {
        if (field == null || field.equals("")) {
            return;
        }
        this.metricAgg = new MetricAgg(field, metric);

        this.aggName = new String[1];
        AggregationBuilder metricBuilder = null;

        this.aggName[0] = "agg_" + field;

        // 遍历追加
        // min 最小值
        if (metric.equals(MetricEnum.MIN)) {
            metricBuilder = AggregationBuilders.min("agg_" + field).field(field);

        } else if (metric.equals(MetricEnum.MAX)) {
            metricBuilder = AggregationBuilders.max("agg_" + field).field(field);
        } else if (metric.equals(MetricEnum.AVG)) {
            metricBuilder = AggregationBuilders.avg("agg_" + field).field(field);
        } else if (metric.equals(MetricEnum.SUM)) {
            metricBuilder = AggregationBuilders.sum("agg_" + field).field(field);
        } else {
            metricBuilder = AggregationBuilders.stats("agg_" + field).field(field);
        }
        this.searchbuilder.addAggregation(metricBuilder);
    }

    /**
     * 自定义 聚合条件 灵活使用
     * <p>
     * 结果总 response 中 获取
     *
     * @param aggregation
     */
    public void addDefinedAggExpression(AggregationBuilder aggregation) {
        if (aggregation != null) {
            this.searchbuilder.addAggregation(aggregation);
        }
    }

    /**
     * 查询结果 按 字段进行 升序 或降序 排列
     *
     * @param field
     * @param order
     */
    public void addSortField(String field, SortOrder order) {
        this.searchbuilder.addSort(field, order);
    }

    /**
     * 设置 取数据的 游标
     *
     * @param start
     */
    public void setStart(int start) {
        this.start = start;
        this.searchbuilder.setFrom(start);
    }

    /**
     * 设置返回值长度
     * 当 rows <=10000 时  返回个数 为 rows
     * 当 rows > 10000 时，返回的个数 为 ---> 游标积累总数 首次大于 rows 的值
     * 当 聚合时 应设置 此值= 0 ，
     * 此值 不影响聚合 agg 结果，但当此值 大于 0 时，会增大 系统的IO压力
     *
     * @param rows
     */
    public void setRow(int rows) {
        this.rows = rows;
        this.searchbuilder.setSize(rows);
    }

    /**
     * 设置 scroll 每批次取得 数量
     *
     * @param scrollBatchNum
     */
    public void setScrollBatchNum(int scrollBatchNum) {
        this.scrollBatchNum = scrollBatchNum;
    }

    /**
     * 设置 查询赋值（到 fields）权利
     */
    public void setGetSourceFlag() {
        this.getSourceFlag = true;
    }

    public void execute() {
        if (this.rows == -1) {
            setRow(100);
        }
        String[] fields = {"_id"};
        this.fields = fields;
        execute(fields);
    }

    public void execute(String[] fields) {
        if (this.queryStringBuilder != null)
            this.searchbuilder.setQuery(this.queryStringBuilder);
        else {
            this.searchbuilder.setQuery(this.query);
        }
        try {
            //commonSearch(fields); 自动分流
            if ((this.start + this.rows) <= 100000) {
                commonSearch(fields);
            } else {
                scrollQuery(fields, scrollBatchNum, null, null);
            }

        } catch (Exception e) {
            this.logger.warn(e.getMessage());
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException localInterruptedException) {
                System.out.println("es 执行失败，重复执行->" + e);
            }
            executeRetry();
        }

    }

    /**
     * 大数据量 情况下,应使用 scroll方式去查询
     */
    public void executeByScroll() {
        String[] fields = {"_id"};
        this.fields = fields;
        executeByScroll(fields);
    }

    /**
     * 大数据量 情况下,应使用 scroll方式去查询
     * , String sortField, SortOrder sortOrder
     */
    public void executeByScroll(String[] fields) {

        this.isExecuteByScroll = true;
        if (this.queryStringBuilder != null) {
            this.searchbuilder.setQuery(this.queryStringBuilder);
        } else {
            this.searchbuilder.setQuery(this.query);
        }
        try {
            scrollQuery(fields, scrollBatchNum, null, null);
        } catch (Exception e) {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException localInterruptedException) {
            }
            System.out.println("es 执行失败，重复执行->" + e);
            executeRetry();
        }

    }

    /**
     * 常规查询方法
     *
     * @param fields
     */
    private void commonSearch(String[] fields) {
        long start = System.currentTimeMillis();
        if ((fields != null) && ((!this.getSourceFlag)) // (!(this.getSourceFlag))
                && (((this.fields == null) || (!(Arrays.equals(fields, this.fields)))))) {
            this.fields = fields;
            // 设置返回的字段值，避免整个 source 全部拿出 造成 IO 压力
            this.searchbuilder = this.searchbuilder.storedFields(fields);
        }

        this.response = ((SearchResponse) this.searchbuilder.execute().actionGet());// _source
        this.responseList.add(this.response);// 方便管理
        long end = System.currentTimeMillis();
        if (isShowExecTime) {
            System.out.println(" 查询 共命中:" + this.getTotal() + "共耗时:" + (end - start) + "毫秒!");
        }
    }


    /**
     * scroll 查询
     * <p>
     * 可通过设置字段 排序方式
     *
     * @param size 每次 滑动 所取得 最大 个数 size * 分片数
     */
    private void scrollQuery(String[] fields, int size, String sortField, SortOrder sortOrder) {

        int allSize = 0;// 返回值 总数
        long start = System.currentTimeMillis();

        if ((fields != null) && ((!this.getSourceFlag)) // (!(this.getSourceFlag))
                && (((this.fields == null) || (!(Arrays.equals(fields, this.fields)))))) {
            this.fields = fields;
            // 设置返回的字段值，避免整个 source 全部拿出 造成 IO 压力
            this.searchbuilder = this.searchbuilder.storedFields(fields);
        }
        SearchResponse scrollResp = null;
        if (sortField == null) {
            scrollResp = this.searchbuilder.setScroll(new TimeValue(60000)).setSize(size)
                    .setQuery(this.getQueryBuiler()).get();
        } else {
            scrollResp = this.searchbuilder.setScroll(new TimeValue(60000)).setSize(size).addSort(sortField, sortOrder == null ? SortOrder.DESC : sortOrder)
                    .setQuery(this.getQueryBuiler()).get();
        }
        //max of 100 hits will be returned for each scroll
        //Scroll until no hits are returned
        do {
  		   /* for (SearchHit hit : scrollResp.getHits().getHits()) {
  		        //Handle the hit...
  		    }*/
            int length = scrollResp.getHits().getHits().length;
            if (length > 0) {

                this.responseList.add(scrollResp);
            }
            allSize += length;
            if (allSize >= (this.start + this.rows)) break;
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();

        } while (scrollResp.getHits().getHits().length != 0); // Zero hits mark the end of the scroll and the while loop.

        long end = System.currentTimeMillis();

        System.out.println("scroll 方式 查询 共命中:" + allSize + "共耗时:" + (end - start) + "毫秒!");

    }

    /**
     * 重新执行一次
     */
    private void executeRetry() {
        try {
            this.response = ((SearchResponse) this.searchbuilder.execute().actionGet());
        } catch (Exception e) {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException localInterruptedException) {

            }
            e.printStackTrace();
            //	executeRetry();
        }
    }

    /**
     * 从 search 返回的 结果中 组装 值
     * 前提是 你建立mapping 的时候要设置 store:true
     * 再由 fields 过滤取值
     *
     * @return
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public List<String[]> getResults() {
        List resultList = new ArrayList();
        if ((this.fields == null) || (this.fields.length == 0)) {
            this.fields = new String[]{"_id"};
        }

        List<SearchResponse> responseBackList = this.responseList;

        int index = 0;
        for (SearchResponse searchResponse : responseBackList) {

            for (SearchHit hit : searchResponse.getHits()) {

                String[] data = new String[this.fields.length];
                int fieldIndex = 0;
                if ((this.types == null) || (this.types.length > 1)) {
                    data = new String[this.fields.length + 1];
                    data[(fieldIndex++)] = hit.getType();
                }
                for (int j = 0; j < this.fields.length; ++j) {
                    if (this.fields[j].equalsIgnoreCase("_id")) {
                        data[(fieldIndex++)] = hit.getId();
                    } else if (this.fields[j].equalsIgnoreCase("_score")) {
                        data[(fieldIndex++)] = Float.toString(hit.getScore());
                    } else if (this.fields[j].equalsIgnoreCase("_index")) {
                        data[(fieldIndex++)] = hit.getIndex();
                    } else if (this.fields[j].equalsIgnoreCase("_type")) {
                        data[(fieldIndex++)] = hit.getType();
                    } else if (hit.getFields().get(this.fields[j]) == null) {
                        data[(fieldIndex++)] = null;
                    } else {
                        data[(fieldIndex++)] = ((SearchHitField) hit.getFields().get(this.fields[j])).getValue().toString();
                    }
                }
                // 说明是通过scroll 方式 执行的，此时 要进行分页
                if (this.isExecuteByScroll) {
                    if (index >= this.start && index < (this.start + this.rows)) {
                        resultList.add(data);
                    }
                } else {
                    resultList.add(data);
                }
                index++;
            }
        }

        return resultList;
    }

    /**
     * 字段高亮功能
     *
     * @param fieldValue 高亮字段值
     * @return
     */
    public String getHighLightStr(String fieldValue) {
        if (fieldValue == null || this.hightLightContentArr == null) {
            return null;
        } else {
            fieldValue = fieldValue.replaceAll("\n", "").replaceAll("\t", "").replaceAll("$nbsp", "").trim();//.replaceAll(" ", "")
            //String replaceStrVal = null;
            for (String str : this.hightLightContentArr) {

                fieldValue = fieldValue.replace(str.trim(), "<span style='color:red'>" + str.trim() + "</span>");//"人民大会堂"
            }
        }
        return fieldValue;
    }

    /**
     * 获取全部命中的 source
     *
     * @return
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public List<String> getDataSource() {
        List resultList = new ArrayList();

        List<SearchResponse> responseBackList = this.responseList;
        for (SearchResponse searchResponse : responseBackList) {
            for (SearchHit hit : searchResponse.getHits()) {
                resultList.add(hit.getSourceAsString());
            }
        }
        return resultList;
    }

    /**
     * 测试发现当 聚合层级关系 》 4 时，第四级 取值 会出错，
     * 采用 getAggRespose 直接存取
     * 获取多字段 聚合 统计结果
     *
     * @return
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public List<StatResult> getMultFieldStatResult() {
        if ((this.aggName == null) || (this.aggName.length == 0)) {
            return null;
        }
        List resultList = new ArrayList();
        Map aggMap = this.response.getAggregations().asMap();

        Aggregation result = (Aggregation) aggMap.get(this.aggName[0]);
        // 在此处进行分流
        if (result instanceof Terms) {
            Terms terms = (Terms) result;
            resultList = getAggTermResult(terms, 0);
        } else if (result instanceof InternalRange) {
            InternalRange range = (InternalRange) result;
            resultList = getAggRangeResult(range);
        } else if (result instanceof InternalCardinality) {
            InternalCardinality cardinality = (InternalCardinality) result;
            resultList = getAggCardinalityResult(cardinality);
        }

        return resultList;
    }


    /**
     * InternalCardinality 聚合方式
     *
     * @param range
     * @return
     */
    private List<StatResult> getAggCardinalityResult(InternalCardinality cardinality) {
        List resultList = new ArrayList();
        StatResult result = new StatResult(cardinality.getName(), cardinality.getValue());
        resultList.add(result);
        return resultList;
    }

    /**
     * InternalRange 聚合方式
     *
     * @param range
     * @return
     */
    private List<StatResult> getAggRangeResult(InternalRange range) {
        List resultList = new ArrayList();
        Iterator bucketIt = range.getBuckets().iterator();
        while (bucketIt.hasNext()) {
            InternalRange.Bucket bucket = (InternalRange.Bucket) bucketIt.next();
            StatResult result = new StatResult(bucket.getKey(), bucket.getDocCount());
            resultList.add(result);
        }
        return resultList;
    }

    /**
     * 递归调用直至获取全部Agg 的数据
     * Agg 多字段 聚合时  可以 多层
     *
     * @param terms      聚合字段
     * @param fieldIndex
     * @return
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private List<StatResult> getAggTermResult(Terms terms, int fieldIndex) {
        List resultList = new ArrayList();
        if ((terms == null) || (terms.getBuckets() == null)) {
            return resultList;
        }
        Iterator bucketIt = terms.getBuckets().iterator();
        while (bucketIt.hasNext()) {
            Terms.Bucket bucket = (Terms.Bucket) bucketIt.next();
            String key = bucket.getKey().toString();
            long keyCount = bucket.getDocCount();
            StatResult result = new StatResult(key, keyCount);
            if (fieldIndex < this.aggName.length - 1) {
                Terms subTerms = (Terms) bucket.getAggregations().asMap().get(this.aggName[(fieldIndex + 1)]);
                result.setSubResult(getAggTermResult(subTerms, fieldIndex + 1));
            }
            resultList.add(result);
        }
        return resultList;
    }

    /**
     * 度量 聚合结果
     *
     * @return
     */
    public MetricResult getMetricAggResult() {
        if ((this.aggName == null) || (this.aggName.length == 0)) {
            return null;
        }
        double value = 0;
        if (this.metricAgg.getMetric().equals(MetricEnum.MIN)) {
            Min min = this.response.getAggregations().get(this.aggName[0]);
            value = min.getValue();
        } else if (this.metricAgg.getMetric().equals(MetricEnum.MAX)) {
            Max max = this.response.getAggregations().get(this.aggName[0]);
            value = max.getValue();
        } else if (this.metricAgg.getMetric().equals(MetricEnum.AVG)) {
            Avg avg = this.response.getAggregations().get(this.aggName[0]);
            value = avg.getValue();
        } else if (this.metricAgg.getMetric().equals(MetricEnum.SUM)) {
            Sum sum = this.response.getAggregations().get(this.aggName[0]);
            value = sum.getValue();
        } else {
            //Stats stats = this.response.getAggregations().get(this.aggName[0]);
            return null;
        }
        return new MetricResult(this.aggName[0], value);
    }

    /**
     * 获取 二级 聚合结果
     *
     * @return
     */
    public List<StatResult> getFieldStatByDateResult() {
        if ((this.aggName == null) || (this.aggName.length != 2)) {
            return null;
        }
        List<StatResult> resultList = new ArrayList<StatResult>();
        Map<String, Aggregation> aggMap = this.response.getAggregations().asMap();
        // 第一级别聚合 值
        Terms terms = (Terms) aggMap.get(this.aggName[0]);
        Iterator<Terms.Bucket> iterator = terms.getBuckets().iterator();
        StatResult statResult = null;
        StatResult subResult = null;
        // 遍历取值
        while (iterator.hasNext()) {
            Terms.Bucket next = iterator.next();
            Object key1 = next.getKey();

            if (key1 == null) {
                continue;
            }
            statResult = new StatResult(key1.toString(), next.getDocCount());
            Map<String, Aggregation> asMap2 = next.getAggregations().asMap();
            Histogram dateAgg = (Histogram) asMap2.get(this.aggName[1]);
            Iterator<? extends Histogram.Bucket> iterator2 = dateAgg.getBuckets().iterator();
            List<StatResult> subList = new ArrayList<StatResult>();
            while (iterator2.hasNext()) {
                Histogram.Bucket next2 = iterator2.next();
                String key2 = next2.getKeyAsString();
                if (key2 == null) {
                    continue;
                }
                subResult = new StatResult(key2.toString(), next2.getDocCount());
                subList.add(subResult);
            }
            if (subList != null && subList.size() > 0) {
                statResult.setSubResult(subList);
            }
            resultList.add(statResult);
        }
        return resultList;
    }

    /**
     * 获取 按日期 聚合结果
     *
     * @return
     */
    public List<StatResult> getStatByDateResult() {
        if ((this.aggName == null) || (this.aggName.length != 1)) {
            return null;
        }
        List<StatResult> resultList = new ArrayList<StatResult>();
        Map<String, Aggregation> aggMap = this.response.getAggregations().asMap();
        Histogram dateAgg = (Histogram) aggMap.get(this.aggName[0]);

        Iterator<? extends Histogram.Bucket> iterator = dateAgg.getBuckets().iterator();
        // StatResult statResult = null;
        while (iterator.hasNext()) {

            Histogram.Bucket next = iterator.next();
            //Object key = next.getKey();
            String key = next.getKeyAsString();
            if (key == null) {
                continue;
            }
            resultList.add(new StatResult(key.toString(), next.getDocCount()));
        }
        return resultList;
    }

    /**
     * 获取 命中个数
     *
     * @return
     */
    public long getTotal() {
        long resultNum = 0L;
        //resultNum = this.response.getHits().getTotalHits();
        List<SearchResponse> responseBackList = this.responseList;
        for (SearchResponse searchResponse : responseBackList) {
            resultNum += searchResponse.getHits().getTotalHits();
        }
        return resultNum;
    }

    /**
     * 获取查询条件
     *
     * @return
     */
    public BoolQueryBuilder getQueryBuiler() {
        return this.query;
    }

    /**
     * 获取 rows< 10000 行时的 response
     *
     * @return
     */
    public SearchResponse getAggResponse() {
        return this.response;
    }

    /**
     * 获取 client 连接
     *
     * @return
     */
    public Client getClient() {
        return this.client;
    }


    /**
     * 检验字符是否合法
     *
     * @param str
     * @return
     */
    private boolean checkStr(Object str) {
        return ((str != null) && (str.toString().trim().length() != 0));
    }
}