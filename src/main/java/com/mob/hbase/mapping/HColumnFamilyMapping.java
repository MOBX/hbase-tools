/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.hbase.mapping;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.BloomType;

/**
 * 列族配置信息
 * 
 * @author zxc Aug 28, 2015 11:46:11 AM
 */
public class HColumnFamilyMapping {

    private String                      familyName;                                         // 列族名称
    private String                      tableName;                                          // htable 名称
    // private String qualifier;
    private Algorithm                   compression = Algorithm.NONE;                       // 压缩算法
    private boolean                     blockCache  = false;                                // 是否使用缓存块
    private int                         blockSize   = -1;                                   // 块大小 -1默认
    private BloomType                   bloomFilter = BloomType.NONE;                       // 布隆算法
    private int                         maxVersions = -1;
    private int                         timeToLive  = -1;
    private boolean                     inMemory    = false;                                // 存放内存

    private Map<String, HColumnMapping> columns     = new HashMap<String, HColumnMapping>(); // 列族下的列

    private String                      keyField;                                           // 行键对应的模型名称

    public HColumnFamilyMapping(String familyName, String tableName, Algorithm compression, boolean blockCache,
                                int blockSize, BloomType bloomFilter, int maxVersions, int timeToLive, boolean inMemory) {
        super();
        this.familyName = familyName;
        this.tableName = tableName;
        this.compression = compression;
        this.blockCache = blockCache;
        this.blockSize = blockSize;
        this.bloomFilter = bloomFilter;
        this.maxVersions = maxVersions;
        this.timeToLive = timeToLive;
        this.inMemory = inMemory;
    }

    public HColumnFamilyMapping addColumnProps(String filedName, HColumnMapping col) {
        if (col != null) columns.put(filedName, col);
        return this;
    }

    public HColumnFamilyMapping addColumns(Map<String, HColumnMapping> cols) {
        columns.putAll(cols);
        return this;
    }

    public HColumnMapping getColumn(String columnName) {
        return columns.get(columnName);
    }

    public HColumnFamilyMapping(String familyName, String tableName) {
        this.familyName = familyName;
        this.tableName = tableName;
    }

    public HColumnDescriptor build() {
        HColumnDescriptor columnDescriptor = new HColumnDescriptor(familyName);

        if (compression != null) columnDescriptor.setCompressionType(compression);
        if (blockSize != -1) columnDescriptor.setBlocksize(blockSize);
        if (bloomFilter != null) columnDescriptor.setBloomFilterType(bloomFilter);
        if (maxVersions != -1) columnDescriptor.setMaxVersions(maxVersions);
        if (timeToLive != -1) columnDescriptor.setTimeToLive(timeToLive);
        columnDescriptor.setInMemory(inMemory);
        columnDescriptor.setBlockCacheEnabled(blockCache);
        return columnDescriptor;

    }

    public String getFamilyName() {
        return familyName;
    }

    public void setFamilyName(String familyName) {
        this.familyName = familyName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Algorithm getCompression() {
        return compression;
    }

    public void setCompression(Algorithm compression) {
        this.compression = compression;
    }

    public boolean isBlockCache() {
        return blockCache;
    }

    public void setBlockCache(boolean blockCache) {
        this.blockCache = blockCache;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    public BloomType getBloomFilter() {
        return bloomFilter;
    }

    public void setBloomFilter(BloomType bloomFilter) {
        this.bloomFilter = bloomFilter;
    }

    public int getMaxVersions() {
        return maxVersions;
    }

    public void setMaxVersions(int maxVersions) {
        this.maxVersions = maxVersions;
    }

    public int getTimeToLive() {
        return timeToLive;
    }

    public void setTimeToLive(int timeToLive) {
        this.timeToLive = timeToLive;
    }

    public boolean isInMemory() {
        return inMemory;
    }

    public void setInMemory(boolean inMemory) {
        this.inMemory = inMemory;
    }

    public String getKeyField() {
        return keyField;
    }

    public void setKeyField(String keyField) {
        this.keyField = keyField;
    }

    public Map<String, HColumnMapping> getColumns() {
        return columns;
    }

    public void setColumns(Map<String, HColumnMapping> columns) {
        this.columns = columns;
    }

}
