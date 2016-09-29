/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.hbase.dao;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.mob.hbase.config.Config;

/**
 * 动态列dao支持
 * 
 * @author zxc Aug 28, 2015 11:46:11 AM
 */
@SuppressWarnings("deprecation")
public abstract class DynaDAOSupport<T> extends DAOSupport<T> {

    public DynaDAOSupport(String tableName, String family, Class<T> clazz) {
        super(tableName, family, clazz);
    }

    /**
     * dynamic model save
     * 
     * @param rowKey
     * @param data
     * @throws IOException
     */
    public void dynaSave(String rowKey, Map<String, byte[]> data) throws IOException {

        HTableInterface table = null;
        try {
            table = Config.getInstance().getHTable(this.tableName);
            table.setAutoFlush(false);
            Put p = getPut(rowKey, data);
            table.put(p);
            table.flushCommits();
        } finally {
            if (table != null) Config.getInstance().pool.putTable(table);
        }
    }

    public void dynaSaves(Map<String, Map<String, byte[]>> rowDatas) throws IOException {
        if (rowDatas == null || rowDatas.isEmpty()) return;

        HTableInterface table = null;
        try {
            table = Config.getInstance().getHTable(this.tableName);
            table.setAutoFlush(false);
            List<Put> puts = new ArrayList<Put>();
            for (String rowKey : rowDatas.keySet()) {
                puts.add(getPut(rowKey, rowDatas.get(rowKey)));
            }
            table.put(puts);
            table.flushCommits();
        } finally {
            if (table != null) Config.getInstance().pool.putTable(table);
        }
    }

    /**
     * 获取动态列
     * 
     * @param rowKey
     * @return
     * @throws IOException
     */
    public Map<String, byte[]> dynaGet(String rowKey) throws IOException {

        HTableInterface table = null;
        try {
            table = Config.getInstance().getHTable(this.tableName);

            Get get = new Get(Bytes.toBytes(rowKey));
            get.addFamily(this.family);
            Result result = table.get(get);
            Map<String, byte[]> data = handlerMapResult(result);
            return data;
        } finally {
            if (table != null) Config.getInstance().pool.putTable(table);
        }
    }

    /**
     * 组装处理返回结果
     * 
     * @param result
     * @return
     */
    public Map<String, byte[]> handlerMapResult(Result result) {
        Map<String, byte[]> map = new LinkedHashMap<String, byte[]>();
        List<Cell> list = result.listCells();
        KeyValue kv = null;
        if (list != null && !list.isEmpty()) {
            for (Cell cell : list) {
                kv = (KeyValue) cell;
                map.put(Bytes.toString(kv.getQualifier()), kv.getValue());
            }
        }
        return map;
    }

    @SuppressWarnings("rawtypes")
    public List<Map<String, byte[]>> findDynaByQuery(HbaseQuery query) throws Exception {

        HTableInterface table = null;
        try {
            table = Config.getInstance().getHTable(this.tableName);
            Scan scan = new Scan();
            scan.addFamily(this.family);
            if (query != null) {
                FilterList fList = query.build();
                scan.setFilter(fList);
            }
            ResultScanner rs = table.getScanner(scan);
            List<Map<String, byte[]>> results = new ArrayList<Map<String, byte[]>>();
            for (Result r : rs) {
                if (r.isEmpty()) {
                    continue;
                }
                Map<String, byte[]> data = handlerMapResult(r);
                results.add(data);
            }
            return results;
        } finally {
            if (table != null) Config.getInstance().pool.putTable(table);
        }
    }

    public Map<String, Map<String, byte[]>> dynaGets(String... rowKeys) throws IOException, InstantiationException,
                                                                       IllegalAccessException, ClassNotFoundException {
        Map<String, Map<String, byte[]>> results = new TreeMap<String, Map<String, byte[]>>();
        HTableInterface table = null;
        try {
            List<Get> getOps = new ArrayList<Get>();
            for (String key : rowKeys) {
                Get get = new Get(Bytes.toBytes(key));
                get.addFamily(this.family);
                getOps.add(get);
            }
            if (getOps.isEmpty()) {
                return results;
            }
            table = Config.getInstance().getHTable(this.tableName);
            Result[] result = table.get(getOps);
            for (Result r : result) {
                if (r.isEmpty()) {
                    continue;
                }
                Map<String, byte[]> data = handlerMapResult(r);

                results.put(Bytes.toString(r.getRow()), data);
            }
            return results;
        } finally {
            if (table != null) Config.getInstance().pool.putTable(table);
        }
    }

    public Map<String, Map<String, byte[]>> findDynaAll() throws Exception {
        Map<String, Map<String, byte[]>> results = new TreeMap<String, Map<String, byte[]>>();
        HTableInterface table = null;
        try {
            table = Config.getInstance().getHTable(this.tableName);
            Scan scan = new Scan();
            ResultScanner rs = table.getScanner(scan);
            for (Result r : rs) {
                if (r.isEmpty()) {
                    continue;
                }
                Map<String, byte[]> data = handlerMapResult(r);
                results.put(Bytes.toString(r.getRow()), data);
            }
            return results;
        } finally {
            if (table != null) Config.getInstance().pool.putTable(table);
        }
    }

    public Map<String, Map<String, byte[]>> findDynaByRange(byte[] startRow, byte[] endRow) throws Exception {

        HTableInterface table = null;
        try {
            table = Config.getInstance().getHTable(this.tableName);
            Scan scan = new Scan();
            scan.addFamily(this.family);
            if (startRow != null) {
                scan.setStartRow(startRow);
            }
            if (endRow != null) {
                scan.setStopRow(endRow);
            }
            ResultScanner rs = table.getScanner(scan);
            Map<String, Map<String, byte[]>> results = new LinkedHashMap<String, Map<String, byte[]>>();
            for (Result r : rs) {
                results.put(Bytes.toString(r.getRow()), handlerMapResult(r));
            }
            return results;
        } finally {
            if (table != null) Config.getInstance().pool.putTable(table);
        }
    }

    public Map<String, Map<String, byte[]>> findDynaByRange(byte[] startRow, byte[] endRow, int limit) throws Exception {

        HTableInterface table = null;
        try {
            table = Config.getInstance().getHTable(this.tableName);
            Scan scan = new Scan();
            scan.addFamily(this.family);
            if (startRow != null) {
                scan.setStartRow(startRow);
            }
            if (endRow != null) {
                scan.setStopRow(endRow);
            }

            // if(startRow == null){
            scan.setMaxResultSize(limit);
            scan.setFilter(new PageFilter(limit));
            ResultScanner rs = table.getScanner(scan);
            Map<String, Map<String, byte[]>> results = new TreeMap<String, Map<String, byte[]>>();
            for (Result r : rs) {
                results.put(Bytes.toString(r.getRow()), handlerMapResult(r));
            }
            return results;
        } finally {
            if (table != null) Config.getInstance().pool.putTable(table);
        }
    }
}
