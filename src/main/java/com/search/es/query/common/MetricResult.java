package com.search.es.query.common;

/**
 * 度量查询封装结果类
 */
public class MetricResult {

    private String key;
    private double value;


    public MetricResult() {

    }

    public MetricResult(String key, double value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }


}
