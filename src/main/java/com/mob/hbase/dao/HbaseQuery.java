/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.hbase.dao;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 查询条件
 * 
 * @author zxc Aug 28, 2015 11:46:11 AM
 */
public class HbaseQuery<T> {

    private byte[]                   familyName;

    private List<HbaseQueryField<T>> conditions = new ArrayList<HbaseQueryField<T>>();

    public HbaseQuery(String familyName) {
        this.familyName = Bytes.toBytes(familyName);
    }

    public HbaseQuery(byte[] familyName) {
        this.familyName = familyName;
    }

    public HbaseQuery<T> set(String column, byte[] o, CompareOp op) {
        HbaseQueryField<T> qf = new HbaseQueryField<T>(this, column, o, op);
        conditions.add(qf);
        return this;
    }

    public HbaseQueryField<T> field(String column) {
        HbaseQueryField<T> qf = new HbaseQueryField<T>(column, this);
        return qf;
    }

    protected HbaseQuery<T> addConditions(HbaseQueryField<T> condition) {
        conditions.add(condition);
        return this;
    }

    public FilterList build() {
        List<Filter> filters = new ArrayList<Filter>();
        if (conditions.isEmpty()) {
            return new FilterList(filters);
        }
        for (HbaseQueryField<T> field : conditions) {
            Filter fl = new SingleColumnValueFilter(familyName, Bytes.toBytes(field.getName()), field.getCompareOp(),
                                                    field.getValue());
            filters.add(fl);
        }
        FilterList fs = new FilterList(filters);
        return fs;

    }

    public byte[] getFamilyName() {
        return familyName;
    }

    public void setFamilyName(byte[] familyName) {
        this.familyName = familyName;
    }
}
