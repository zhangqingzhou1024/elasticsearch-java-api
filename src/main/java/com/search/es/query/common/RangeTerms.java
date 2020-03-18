package com.search.es.query.common;

/**
 * 范围聚合封装
 */
public class RangeTerms {
    //聚合字段
    private String termName;
    //范围起始条件
    private double from;
    //范围结束条件
    private double to;

    public RangeTerms(double from, double to) {
        this.from = from;
        this.to = to;
        if (this.from == -1.0D) {
            this.termName = "0~" + this.to;
        } else if (this.to == -1.0D) {
            this.termName = this.from + "~";
        } else {
            this.termName = this.from + "~" + this.to;
        }
    }

    public RangeTerms(String termName, double from, double to) {
        this.termName = termName;
        this.from = from;
        this.to = to;
    }

    public double getFrom() {
        return this.from;
    }

    public void setFrom(double from) {
        this.from = from;
    }

    public double getTo() {
        return this.to;
    }

    public void setTo(double to) {
        this.to = to;
    }

    public String getTermName() {
        if (this.termName == null) {
            if (this.from == -1.0D) {
                this.termName = "0~" + this.to;
            } else if (this.to == -1.0D) {
                this.termName = this.from + "~";
            } else {
                this.termName = this.from + "~" + this.to;
            }
        }
        return this.termName;
    }

    public void setTermName(String termName) {
        this.termName = termName;
    }
}