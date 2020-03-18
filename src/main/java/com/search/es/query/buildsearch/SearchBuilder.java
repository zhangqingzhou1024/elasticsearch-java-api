package com.search.es.query.buildsearch;


import com.search.es.query.common.*;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.range.InternalRange;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.*;


/**
 * elasticsearch - java
 * <p>
 * 查询常用方法 封装 类
 */
public class SearchBuilder {
   // private Logger logger = Logger.getLogger(searchRequestBuilder.class);
    /**
     * 字段 数据
     */
    private String[] fields;
    /**
     * 索引值
     */
    private String index;
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
    private SearchRequestBuilder searchRequestBuilder;

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
     * BoolQueryBuilder
     */
    private BoolQueryBuilder query;
    //private BoolFilterBuilder boolFilter;  新版本已经放弃
    /**
     * 查询条件-组合 token方式
     */
    private QueryStringQueryBuilder queryStringBuilder;

    /**
     * 设置 返回结果的 size
     * 初始化值 -1
     */
    private int rows = -1;
    /**
     * facetName
     */
    private String facetName = "elastic-facet";
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
     * @param index 索引名称--相当于 mysql 中 数据库
     * @param type  类型---->相当于 mysql 中 table  单 类型
     *              集群信息 读取配置文件
     */
    public SearchBuilder(String index, String type) {
        String[] types = {type};
        if (type == null) {
            types = null;
        }
        initCoreServer(index, types);
    }


    /**
     * 初始化 es-java client
     *
     * @param index 索引名称--相当于 mysql 中 数据库
     * @param types 类型---->相当于 mysql 中 table
     */
    private void initCoreServer(String index, String[] types) {
        this.index = index;
        this.types = types;
        try {
            //设置集群名称
            this.client = TransportClientFactory.getEsClient();
            initResponse();
        } catch (Exception e) {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException localInterruptedException) {
            }
            initCoreServer(index, types);
        }
    }

    /**
     * 初始化结果集
     */
    private void initResponse() {
        this.fields = null;
        this.query = new BoolQueryBuilder();
        this.responseList = new ArrayList();
        this.queryStringBuilder = null;
        this.getSourceFlag = false;
        if ((this.index == null) || (this.index.length() == 0)) {
            this.searchRequestBuilder = this.client.prepareSearch(new String[0]);
        } else {
            this.searchRequestBuilder = this.client.prepareSearch(new String[]{this.index});
        }
        if ((this.types != null) && (this.types.length > 0)) {
            this.searchRequestBuilder = this.searchRequestBuilder.setTypes(this.types);
        }
    }

    /**
     * 多次执行时，应在执行之前 重设一下
     * 清空 上次 记录
     */
    public void reset() {
        initResponse();
        this.rows = -1;
        this.aggName = null;
        this.fields = null;
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
     * @param field    列名 -- 多列
     * @param keywords 查询值
     * @param operator 表示 与其它 查询条件 的关系
     * @param combine  拼接方式 and/or链式条件 拼装方式
     *                 <p>
     *                 http://www.tuicool.com/articles/qqymmaJ  多字段匹配
     */
    public void addKeywordsQuery(String field, String keywords, QueryTypeEnum operator, KeywordsCombineEnum combine) {
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
        addQuery(matchBuilder, operator);
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
     * @param fields   列名 -- 多列
     * @param keywords 查询值
     * @param operator 表示 与其它 查询条件 的关系
     * @param combine  拼接方式 and/or链式条件 拼装方式
     *                 <p>
     *                 http://www.tuicool.com/articles/qqymmaJ  多字段匹配
     */
    public void addKeywordsQuery(String[] fields, String keywords, QueryTypeEnum operator, KeywordsCombineEnum combine) {
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
        addQuery(match, operator);
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
     * @param keywords
     * @param operator 相关文档
     *                 http://www.blogjava.net/persister/archive/2009/07/14/286634.html
     */
    public void addPhraseQuery(String field, String keywords, QueryTypeEnum operator) {
        if ((!(checkStr(field))) || (!(checkStr(keywords)))) {
            return;
        }
        MatchPhraseQueryBuilder match = QueryBuilders.matchPhraseQuery(field, keywords);
        //	QueryBuilders.m
        addQuery(match, operator);
    }

    /**
     * 短语查询 -- 增加位置的容忍度
     *
     * @param field
     * @param phrase
     * @param slop     移动次数（位置偏移量-相对于token）
     * @param operator 查询方式：
     *                 第一步：先把 keywords 查询值 按照分词器 分成 tokens
     *                 第二步：然后把 tokens 与 fields-value 字段值 分成的 tokens 进行匹配
     *                 当没有 slop (对位置容忍度放大)时，tokens 必须一致 才会返回文档
     *                 当值 、顺序位置 都匹配上时 返回文档，
     *                 eg：
     *                 首先，强调一下PhraseQuery对象，这个对象不属于跨度查询类，但能完成跨度查询功能。
     *                 匹配到的文档所包含的项通常是彼此相邻的，考虑到原文档中在查询项之间可能有一些中间项，
     *                 或为了能查询倒排的项，PhraseQuery设置了slop因子，但是这个slop因子指2个项允许最大间隔距离，
     *                 不是传统意义上的距离，是按顺序组成给定的短语，所需要移动位置的次数，
     *                 这表示PhraseQuery是必须按照项在文档中出现的顺序计算跨度的，如quick brown fox为文档，
     *                 则quick fox2个项的slop为1，quick向后移动一次.而fox quick需要quick向后移动3次，所以slop为3
     */
    public void addPhrasePrefixQuery(String field, String phrase, int slop, QueryTypeEnum operator) {
        if ((!(checkStr(field))) || (!(checkStr(phrase)))) {
            return;
        }
        MatchPhrasePrefixQueryBuilder match = QueryBuilders.matchPhrasePrefixQuery(field, phrase).slop(slop);
        addQuery(match, operator);
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
     * @param term     值
     * @param operator 表示 与其它 查询条件 的关系
     */
    public void addPrimitiveTermQuery(String field, Object term, QueryTypeEnum operator) {
        if ((!(checkStr(field))) || (!(checkStr(term)))) {
            return;
        }
        addQuery(QueryBuilders.termQuery(field, term.toString().trim()), operator);
    }

    /**
     * 多 term匹配是对QueryStringQueryBuilder 的进行的封装
     * 匹配步骤：
     * 第一步：把多个terms 和在一起 然后进行 分词拆分->tokens
     * 第二步：去 field-value 中的 tokens 库中去匹配，
     * 只要有一个匹配上 就返回回档
     *
     * @param field    字段
     * @param terms    匹配值数组
     * @param operator 与其他查询条件的关系
     */
    public void addPrimitiveTermQuery(String field, String[] terms, QueryTypeEnum operator) {
        if ((!(checkStr(field))) || (terms == null) || (terms.length == 0)) {
            return;
        }
        String term = "";
        for (int i = 0; i < terms.length; ++i) {
            term = term + terms[i];
            if (i < terms.length - 1) {
                term = term + " ";
            }
        }
        String queryString = term;
        QueryStringQueryBuilder query = new QueryStringQueryBuilder(queryString);
        query.defaultOperator(QueryStringQueryBuilder.DEFAULT_OPERATOR);
        query.defaultField(field);

        addQuery(query, operator);
    }

    /**
     * 支持java 正则表达式
     * 词条（token）级别
     *
     * @param field     字段
     * @param regexpStr 正则表达式
     * @param operator  与其他查询条件的拼接关系
     *                  prefix，wildcard以及regexp查询基于词条(token)进行操作
     *                  http://blog.csdn.net/dm_vincent/article/details/42024799
     */
    public void addRegexpQuery(String field, String regexpStr, QueryTypeEnum operator) {
        if ((!(checkStr(field))) || (!(checkStr(regexpStr)))) {
            return;
        }
        RegexpQueryBuilder regex = new RegexpQueryBuilder(field, regexpStr);
        addQuery(regex, operator);
    }

    /**
     * token 前缀 查询
     * 这个地方的Prefix 是对 filed-value 分词后tokens 所说的
     * 查询步骤：
     * 第一步：整个 term 值会充当一个token的前缀（一个整体 不再分词了）】
     * 第二步：拿到 上面的 值 取 文档库中 匹配 filed -value 中 所有token ,只要满足库中token前缀 与 term 一致时
     * 返回文档
     *
     * @param field    字段
     * @param term     （token）前缀值
     * @param operator 与之前查询条件的 拼接关系
     */
    public void addPrefixQuery(String field, String term, QueryTypeEnum operator) {
        if ((!(checkStr(field))) || (!(checkStr(term)))) {
            return;
        }
        PrefixQueryBuilder prefix = new PrefixQueryBuilder(field, term);
        addQuery(prefix, operator);
    }

    /**
     * 通配符查询
     * 基于 token(词条)
     * eg；"W?FHW
     * 可以把 WkEHW 的查出来
     *
     * @param field
     * @param term
     * @param operator http://blog.csdn.net/dm_vincent/article/details/42024799
     */
    public void addWildcardQuery(String field, String term, QueryTypeEnum operator) {
        if ((!(checkStr(field))) || (!(checkStr(term)))) {
            return;
        }

        WildcardQueryBuilder wildQuery = new WildcardQueryBuilder(field, term);
        addQuery(wildQuery, operator);
    }

    /**
     * 模糊查询
     * 支持的不是很好
     *
     * @param field
     * @param term
     * @param operator
     */
    private void addFuzzyQuery(String field, String term, QueryTypeEnum operator) {
        if ((!(checkStr(field))) || (!(checkStr(term)))) {
            return;
        }
        FuzzyQueryBuilder fuzzyQueryBuilder = new FuzzyQueryBuilder(field, term);
        //WildcardQueryBuilder wildQuery = new WildcardQueryBuilder(field, term);
        addQuery(fuzzyQueryBuilder, operator);
    }

    /**
     * 增加值域范围查询
     *
     * @param field        字段
     * @param startTerm    起始值  无限制 可为 null
     * @param startOperate 规定> or >=
     * @param endTerm      结束值  无限制 可为 null
     * @param endOperate   规定< or <=
     * @param operator     与其他查询条件的关系
     *                     <p>
     *                     如果是时间范围查询，字符格式一定要与 mapping中 格式 一致
     */
    public void addRangeQuery(String field, Object startTerm, RangeCommon startOperate, Object endTerm,
                              RangeCommon endOperate, QueryTypeEnum operator) {
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
        addQuery(builder, operator);
    }

    /**
     * 自定义 QueryBuilder
     * 拼接到 已有的 query
     *
     * @param query
     * @param operator
     */
    public void addQuery(QueryBuilder query, QueryTypeEnum operator) {
        if (operator.equals(QueryTypeEnum.MUST)) {
            this.query.must(query);
        } else if (operator.equals(QueryTypeEnum.MUST_NOT)) {
            this.query.mustNot(query);
        } else {
            this.query.should(query);
        }
    }


    /**
     * 与其它查询条件互斥，设置此种条件时 其他条件不执行
     * 此种方式为 全表查询 ，因为没有 设置 field
     * 查询方式：
     * 第一步：先把查询条件 分词化 生成 tokens-1
     * 第二步：全 type  匹配tokens-2，只要tokens-1 中 有一个 匹配上 就会返回文档
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
    public void addRangeFacet(String field, List<RangeTerms> rangesList) {
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
        this.searchRequestBuilder.addAggregation(builder);
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

        this.searchRequestBuilder.addAggregation(termsBuilder);
    }

    /**
     * 度量聚合
     * 与其它聚合方式 互斥  覆盖现象
     *
     * @param field      字段名称
     * @param metricEnum 度量聚合类型
     */
    public void addMetricAgg(String field, MetricEnum metricEnum) {
        if (field == null || "".equals(field)) {
            return;
        }
        this.metricAgg = new MetricAgg(field, metricEnum);

        this.aggName = new String[1];
        AggregationBuilder MetricEnumBuilder = null;

        this.aggName[0] = "agg_" + field;

        // 遍历追加
        // min 最小值
        if (metricEnum.equals(MetricEnum.MIN)) {
            MetricEnumBuilder = AggregationBuilders.min("agg_" + field).field(field);

        } else if (metricEnum.equals(MetricEnum.MAX)) {
            MetricEnumBuilder = AggregationBuilders.max("agg_" + field).field(field);
        } else if (metricEnum.equals(MetricEnum.AVG)) {
            MetricEnumBuilder = AggregationBuilders.avg("agg_" + field).field(field);
        } else if (metricEnum.equals(MetricEnum.SUM)) {
            MetricEnumBuilder = AggregationBuilders.sum("agg_" + field).field(field);
        } else {
            MetricEnumBuilder = AggregationBuilders.stats("agg_" + field).field(field);
        }
        this.searchRequestBuilder.addAggregation(MetricEnumBuilder);
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
            this.searchRequestBuilder.addAggregation(aggregation);
        }
    }

    /**
     * 查询结果 按 字段进行 升序 或降序 排列
     *
     * @param field
     * @param order
     */
    public void addSortField(String field, SortOrder order) {
        this.searchRequestBuilder.addSort(field, order);

    }

    /**
     * 自定义排序，可实现多级平排序
     *
     * @param sortBuilder
     */
    public void addSortBuilder(SortBuilder sortBuilder) {
        this.searchRequestBuilder.addSort(sortBuilder);
    }

    public void setStart(int start) {
        this.searchRequestBuilder.setFrom(start);
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
        this.searchRequestBuilder.setSize(rows);
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

    /**
     * 执行操作
     *
     * @param fields 返回字段设置
     */
    public void execute(String[] fields) {
        // 当 Row 返回值 >10000 时，应执行 scroll 查询，<= 10000 时  常规查询
        if (this.queryStringBuilder != null) {
            this.searchRequestBuilder.setQuery(this.queryStringBuilder);
        } else {
            this.searchRequestBuilder.setQuery(this.query);
        }

        try {
            if (this.rows <= 10000) {
                commonSearch(fields);
            } else {
                scrollQuery(fields, 5000);
            }

        } catch (Exception e) {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException localInterruptedException) {
                System.out.println("es 执行失败，重复执行->" + e);
            }
            executeRetry();
        }
    }

    /**
     * 常规查询方法
     *
     * @param fields 字段数组
     */
    private void commonSearch(String[] fields) {
        long start = System.currentTimeMillis();
        if ((fields != null) && ((!this.getSourceFlag)) // (!(this.getSourceFlag))
                && (((this.fields == null) || (!(Arrays.equals(fields, this.fields)))))) {
            this.fields = fields;
            // 设置返回的字段值，避免整个 source 全部拿出 造成 IO 压力
            this.searchRequestBuilder = this.searchRequestBuilder.storedFields(fields);
        }

        this.response = ((SearchResponse) this.searchRequestBuilder.execute().actionGet());// _source
        this.responseList.add(this.response);// 方便管理
        long end = System.currentTimeMillis();
        System.out.println(" 查询 共命中:" + this.getTotal() + "共耗时:" + (end - start) + "毫秒!");
    }


    /**
     * scroll 查询
     *
     * @param size 每次 滑动 所取得 最大 个数 size * 分片数
     */
    public void scrollQuery(String[] fields, int size) {

        int allSize = 0;// 返回值 总数
        long start = System.currentTimeMillis();

        if ((fields != null) && ((!this.getSourceFlag)) // (!(this.getSourceFlag))
                && (((this.fields == null) || (!(Arrays.equals(fields, this.fields)))))) {
            this.fields = fields;
            // 设置返回的字段值，避免整个 source 全部拿出 造成 IO 压力
            this.searchRequestBuilder = this.searchRequestBuilder.storedFields(fields);
        }
        SearchResponse scrollResp = this.searchRequestBuilder.setScroll(new TimeValue(60000)).setSize(size).get();

        do {
  		   /* for (SearchHit hit : scrollResp.getHits().getHits()) {
  		        //Handle the hit...
  		    }*/
            int length = scrollResp.getHits().getHits().length;
            if (length > 0) {

                this.responseList.add(scrollResp);
            }
            allSize += length;
            if (allSize >= this.rows) {
                break;
            }
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();

            // this.responseList.add(scrollResp);
        } while (scrollResp.getHits().getHits().length != 0); // Zero hits mark the end of the scroll and the while loop.

        long end = System.currentTimeMillis();

        System.out.println("scroll 方式 查询 共命中:" + allSize + "共耗时:" + (end - start) + "毫秒!");

    }

    /**
     * 重新执行一次
     */
    private void executeRetry() {
        try {
            this.response = ((SearchResponse) this.searchRequestBuilder.execute().actionGet());
        } catch (Exception e) {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException localInterruptedException) {
            }
            executeRetry();
        }
    }

    /**
     * 从 search 返回的 结果中 组装 值
     * 前提是 你建立mapping 的时候要设置 store:true
     * 再由 fields 过滤取值
     *
     * @return 查询结果值
     */
    public List<String[]> getResults() {
        List<String[]> resultList = new ArrayList();
        if ((this.fields == null) || (this.fields.length == 0)) {
            this.fields = new String[]{"_id"};
        }

        List<SearchResponse> responseBackList = this.responseList;
        for (SearchResponse searchResponse : responseBackList) {

            for (SearchHit hit : searchResponse.getHits()) {
                String[] data = new String[this.fields.length];
                int fieldIndex = 0;
                if ((this.types == null) || (this.types.length > 1)) {
                    data = new String[this.fields.length + 1];
                    data[(fieldIndex++)] = hit.getType();
                }
                for (String field : this.fields) {
                    if ("_id".equalsIgnoreCase(field)) {
                        data[(fieldIndex++)] = hit.getId();
                    } else if ("_score".equalsIgnoreCase(field)) {
                        data[(fieldIndex++)] = Float.toString(hit.getScore());
                    } else if ("_index".equalsIgnoreCase(field)) {
                        data[(fieldIndex++)] = hit.getIndex();
                    } else if ("_type".equalsIgnoreCase(field)) {
                        data[(fieldIndex++)] = hit.getType();
                    } else if (hit.getFields().get(field) == null) {
                        data[(fieldIndex++)] = null;
                    } else {
                        data[(fieldIndex++)] = ((SearchHitField) hit.getFields().get(field)).getValue().toString();
                    }
                }
                resultList.add(data);
            }
        }

        return resultList;
    }

    /**
     * 获取全部命中的 source
     *
     * @return 全部命中的source
     */
    public List<String> getDataSource() {
        List<String> resultList = new ArrayList();

        List<SearchResponse> responseBackList = this.responseList;
        for (SearchResponse searchResponse : responseBackList) {
            for (SearchHit hit : searchResponse.getHits()) {
                resultList.add(hit.getSourceAsString());
            }
        }
        return resultList;
    }

    /**
     * 获取多字段聚合统计结果
     *
     * @return 聚合结果
     */
    public List<StatResult> getMultFieldStatResult() {
        if ((this.aggName == null) || (this.aggName.length == 0)) {
            return null;
        }
        List<StatResult> resultList = new ArrayList();
        Map<String, Aggregation> aggMap = this.response.getAggregations().asMap();

        Aggregation result = aggMap.get(this.aggName[0]);
        // 在此处分为两处
        if (result instanceof Terms) {
            Terms terms = (Terms) result;
            resultList = getAggTermResult(terms, 0);
        } else if (result instanceof InternalRange) {
            InternalRange range = (InternalRange) result;
            resultList = getAggRangeResult(range);
        }

        return resultList;
    }

    /**
     * InternalRange 聚合方式
     *
     * @param range
     * @return InternalRange 聚合结果
     */
    private List<StatResult> getAggRangeResult(InternalRange range) {
        List<StatResult> resultList = new ArrayList();
        for (Object o : range.getBuckets()) {
            InternalRange.Bucket bucket = (InternalRange.Bucket) o;
            StatResult result = new StatResult(bucket.getKey(), bucket.getDocCount());
            resultList.add(result);
        }
        return resultList;
    }

    /**
     * 递归调用直至获取全部Agg 的数据
     * Agg多字段聚合时可以多层
     *
     * @param terms      聚合字段
     * @param fieldIndex 聚合索引index
     * @return 聚合结果
     */
    private List<StatResult> getAggTermResult(Terms terms, int fieldIndex) {
        List<StatResult> resultList = new ArrayList();
        if ((terms == null) || (terms.getBuckets() == null)) {
            return resultList;
        }
        for (Terms.Bucket bucket : terms.getBuckets()) {
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
     * 获取度量聚合结果
     *
     * @return 度量聚合结果
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
     * 设置搜索方式
     */
    public void setSearchTypeForStatictics() {
        //SearchType.COUNT 过期
        this.searchRequestBuilder.setSearchType(SearchType.DEFAULT);
    }

    /**
     * 获取 命中个数
     *
     * @return 命中个数
     */
    public long getTotal() {
        long resultNum = 0L;
        List<SearchResponse> responseBackList = this.responseList;
        for (SearchResponse searchResponse : responseBackList) {
            resultNum += searchResponse.getHits().getTotalHits();
        }
        return resultNum;
    }

    /**
     * 获取查询条件
     *
     * @return BoolQueryBuilder
     */
    public BoolQueryBuilder getQueryBuiler() {
        return this.query;
    }

    /**
     * 获取查询结果值
     *
     * @return SearchResponse 查询结果值
     */
    public SearchResponse SearchResponse() {
        return this.response;
    }

    /**
     * 检验字符是否合法
     *
     * @param str 字符
     * @return 是否合法
     */
    private boolean checkStr(Object str) {
        return ((str != null) && (str.toString().trim().length() != 0));
    }
}