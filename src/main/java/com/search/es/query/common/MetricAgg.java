package com.search.es.query.common;

/**
 * 度量聚合 封装类
 */
public class MetricAgg {

    private String field;
    private MetricEnum metric;
    //private SortOrder sortOrder;


    public MetricAgg() {

    }

    public MetricAgg(String field, MetricEnum metric) {
        this.field = field;
        this.metric = metric;
        //this.sortOrder = sortOrder;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public MetricEnum getMetric() {
        return metric;
    }

    public void setMetric(MetricEnum metric) {
        this.metric = metric;
    }


}
