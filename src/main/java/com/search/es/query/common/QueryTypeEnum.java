package com.search.es.query.common;

/**
 * 枚举类 操作方式：
 * MUST    一定为此值  add
 * MUST_NOT    一定不能是此值  not
 * SHOULD    -- 相当于 or
 */
public enum QueryTypeEnum {
    MUST, MUST_NOT, SHOULD;
}