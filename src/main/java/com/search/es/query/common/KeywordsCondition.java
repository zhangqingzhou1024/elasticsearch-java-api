package com.search.es.query.common;

import com.oracle.webservices.internal.api.databinding.DatabindingMode;
import lombok.Data;

/**
 * 多字段 关键词拼接实体类
 * 类似于 Lucene 中 queryString 查询字符串 如 "+title:新中国  +keywords_label:*立案审查"
 * @author zqz
 * @createTime 2018年6月5日
 * @todo TODO
 */
@Data
public class KeywordsCondition{
	// 字段名称
	private String field;
	// 关键词
	private String keywords;
	// 关键词组合方式
	private KeywordsCombineEnum combine;
	
	// 此条信息 的 执行条件，MUST MUST_NOT SHOULD
	private QueryTypeEnum operator;
	/**
	 * 
	 * @param field  字段名称
	 * @param keywords  关键词
	 * @param combine  关键词分词后——各个词 组合方式 AND、OR
	 * @param operator 操作方式 MUST MUST_NOT SHOULT
	 */
	public KeywordsCondition(String field, String keywords, KeywordsCombineEnum combine, QueryTypeEnum operator) {
		this.field = field;
		this.keywords = keywords;
		this.combine = combine;
		this.operator = operator;
	}
	
	public KeywordsCondition(String field, String keywords, KeywordsCombineEnum combine) {
		this.field = field;
		this.keywords = keywords;
		this.combine = combine;
		this.operator = QueryTypeEnum.MUST;
	}
	public KeywordsCondition(String field, String keywords) {
		this.field = field;
		this.keywords = keywords;
		this.combine = KeywordsCombineEnum.OR;
		this.operator = QueryTypeEnum.MUST;

	}


}
