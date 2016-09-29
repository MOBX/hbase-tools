/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.hbase.dao;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

/**
 * 查询字段
 * 
 * @author zxc Aug 28, 2015 11:46:11 AM
 */
public class HbaseQueryField<T> {

    private HbaseQuery<T> query;
    private String        name;
    private byte[]        value;
    private CompareOp     compareOp;

    public HbaseQueryField(String name, HbaseQuery<T> query) {
        this.name = name;
        this.query = query;
    }

    public HbaseQueryField(HbaseQuery<T> query, String name, byte[] value, CompareOp compareOp) {
        this.query = query;
        this.name = name;
        this.value = value;
        this.compareOp = compareOp;
    }

    // =========opts==============

    public HbaseQuery<T> less(byte[] val) {
        this.compareOp = CompareOp.LESS;
        this.value = val;
        return query.addConditions(this);
    }

    public HbaseQuery<T> lessEq(byte[] val) {
        this.compareOp = CompareOp.LESS_OR_EQUAL;
        this.value = val;
        return query.addConditions(this);
    }

    public HbaseQuery<T> eq(byte[] val) {
        this.compareOp = CompareOp.EQUAL;
        this.value = val;
        return query.addConditions(this);
    }

    public HbaseQuery<T> notEq(byte[] val) {
        this.compareOp = CompareOp.NOT_EQUAL;
        this.value = val;
        return query.addConditions(this);
    }

    public HbaseQuery<T> gt(byte[] val) {
        this.compareOp = CompareOp.GREATER;
        this.value = val;
        return query.addConditions(this);
    }

    public HbaseQuery<T> gtEq(byte[] val) {
        this.compareOp = CompareOp.GREATER_OR_EQUAL;
        this.value = val;
        return query.addConditions(this);
    }

    // =======================================

    protected HbaseQuery<T> getQuery() {
        return query;
    }

    protected void setQuery(HbaseQuery<T> query) {
        this.query = query;
    }

    protected String getName() {
        return name;
    }

    protected void setName(String name) {
        this.name = name;
    }

    protected byte[] getValue() {
        return value;
    }

    protected CompareOp getCompareOp() {
        return compareOp;
    }

    protected void setCompareOp(CompareOp compareOp) {
        this.compareOp = compareOp;
    }

    protected void setValue(byte[] value) {
        this.value = value;
    }
}
