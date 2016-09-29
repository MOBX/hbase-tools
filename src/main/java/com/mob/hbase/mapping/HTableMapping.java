/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.hbase.mapping;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.*;

/**
 * Htable 配置模型
 * 
 * @author zxc Aug 28, 2015 11:46:11 AM
 */
public class HTableMapping {

    private String                            tableName;                                             // 表名
    private Map<String, HColumnFamilyMapping> families = new HashMap<String, HColumnFamilyMapping>(); // 映射

    public HTableMapping(String tableName) {
        this.tableName = tableName;
    }

    /**
     * 创建表描述信息
     * 
     * @return
     */
    public HTableDescriptor build() {
        TableName tb = TableName.valueOf(tableName);
        HTableDescriptor hTable = new HTableDescriptor(tb);
        for (String key : families.keySet()) {
            HColumnFamilyMapping mapping = families.get(key);
            HColumnDescriptor des = mapping.build();
            hTable.addFamily(des);
        }
        return hTable;
    }

    public HTableMapping addColumnFamily(HColumnFamilyMapping cmf) {
        families.put(cmf.getFamilyName(), cmf);
        return this;
    }

    public HColumnFamilyMapping getFamlily(String familyName) {
        HColumnFamilyMapping fm = families.get(familyName);
        return fm;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Map<String, HColumnFamilyMapping> getFamilies() {
        return families;
    }

    public void setFamilies(Map<String, HColumnFamilyMapping> families) {
        this.families = families;
    }
}
