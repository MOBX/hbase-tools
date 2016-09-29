/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.hbase.dao;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.lamfire.utils.*;
import com.mob.hbase.config.Config;
import com.mob.hbase.mapping.*;
import com.mob.hbase.pojo.PageHBase;
/**
 * 基本Hbase数据库操作
 * 
 * @author zxc Aug 28, 2015 11:46:11 AM
 */
@SuppressWarnings("deprecation")
public abstract class DAOSupport<T> {

    protected String             tableName;
    protected byte[]             family;
    private HColumnFamilyMapping mapping;
    private Class<T>             clazz;

    public DAOSupport(String tableName, String family, Class<T> clazz) {
        this.tableName = tableName;
        this.family = Bytes.toBytes(family);
        // 获取映射关系
        HColumnFamilyMapping fm = Config.getInstance().getStore().get(tableName).getFamlily(family);
        this.mapping = fm;
        this.clazz = clazz;
    }

    public HbaseQuery<T> createQuery() {
        return new HbaseQuery<T>(family);
    }

    /**
     * 是否存在列
     * 
     * @param name
     * @param val
     * @return
     * @throws Exception
     */
    public boolean exist(String rowKey) throws Exception {
        HTableInterface table = null;
        try {
            table = Config.getInstance().getHTable(this.tableName);
            Get get = new Get(Bytes.toBytes(rowKey));
            // 增加列族过滤
            get.addFamily(family);
            return table.exists(get);
        } catch (Exception e) {
            throw e;
        } finally {
            if (table != null) Config.getInstance().pool.putTable(table);
        }
    }

    /**
     * 根据查询条件查询
     * 
     * @param query
     * @return
     * @throws Exception
     */
    public List<T> findByQuery(HbaseQuery<T> query) throws Exception {

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
            List<T> results = new ArrayList<T>();
            for (Result r : rs) {
                T data = (T) handlerResult(r, this.clazz);
                results.add(data);
            }
            return results;
        } finally {
            if (table != null) Config.getInstance().pool.putTable(table);
        }
    }

    /**
     * 保存对象
     * 
     * @param data
     * @throws IOException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    @SuppressWarnings("unused")
    public void save(T data) throws IOException, IllegalAccessException, InvocationTargetException,
                            NoSuchMethodException {
        HTableInterface table = null;
        try {
            Object rowKey = PropertyUtils.getProperty(data, mapping.getKeyField());
            table = Config.getInstance().getHTable(this.tableName);
            table.setAutoFlush(false);
            Put p = getPut(data);
            table.put(p);
            table.flushCommits();
        } finally {
            if (table != null) Config.getInstance().pool.putTable(table);
        }
    }

    /**
     * 保存多个对象
     * 
     * @param data
     * @throws IOException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    public void saves(List<T> data) throws IOException, IllegalAccessException, InvocationTargetException,
                                   NoSuchMethodException {
        if (data == null || data.isEmpty()) return;
        HTableInterface table = null;
        try {
            List<Put> puts = new ArrayList<Put>();
            table = Config.getInstance().getHTable(this.tableName);
            table.setAutoFlush(false);
            for (T obj : data) {
                puts.add(getPut(obj));
            }
            table.put(puts);
            table.flushCommits();
        } finally {
            if (table != null) Config.getInstance().pool.putTable(table);
        }
    }

    /**
     * 获取对象
     * 
     * @param rowKeys
     * @return
     * @throws IOException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws ClassNotFoundException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws NoSuchFieldException
     * @throws SecurityException
     */
    public List<T> gets(String... rowKeys) throws IOException, InstantiationException, IllegalAccessException,
                                          ClassNotFoundException, SecurityException, NoSuchFieldException,
                                          InvocationTargetException, NoSuchMethodException {
        List<T> results = new ArrayList<T>();
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
                T data = (T) handlerResult(r, this.clazz);
                if (data != null) {
                    results.add(data);
                }
            }
            return results;
        } finally {
            if (table != null) Config.getInstance().pool.putTable(table);
        }
    }

    /**
     * 获取对象
     * 
     * @param rowKeys
     * @return
     * @throws IOException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws ClassNotFoundException
     */
    public Map<String, byte[]> gets(String[] rowKeys, String column) throws IOException, InstantiationException,
                                                                    IllegalAccessException, ClassNotFoundException {
        Map<String, byte[]> results = new LinkedHashMap<String, byte[]>();
        HTableInterface table = null;
        try {
            List<Get> getOps = new ArrayList<Get>();
            byte[] qualifier = Bytes.toBytes(column);
            for (String key : rowKeys) {
                Get get = new Get(Bytes.toBytes(key));
                get.addColumn(this.family, qualifier);
                getOps.add(get);
            }
            if (getOps.isEmpty()) {
                return results;
            }
            table = Config.getInstance().getHTable(this.tableName);
            Result[] result = table.get(getOps);
            for (Result r : result) {
                results.put(Bytes.toString(r.getRow()), r.getValue(this.family, Bytes.toBytes(column)));
            }
            return results;
        } finally {
            if (table != null) Config.getInstance().pool.putTable(table);
        }
    }

    /**
     * 获取多个对象
     * 
     * @param rowKeys
     * @return
     * @throws IOException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws ClassNotFoundException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws NoSuchFieldException
     * @throws SecurityException
     */
    public Map<String, T> mapGets(String... rowKeys) throws IOException, InstantiationException,
                                                    IllegalAccessException, ClassNotFoundException, SecurityException,
                                                    NoSuchFieldException, InvocationTargetException,
                                                    NoSuchMethodException {
        Map<String, T> results = new TreeMap<String, T>();
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
                T data = (T) handlerResult(r, this.clazz);
                if (data != null) {
                    results.put(Bytes.toString(r.getRow()), data);
                }
            }
            return results;
        } finally {
            if (table != null) Config.getInstance().pool.putTable(table);
        }
    }

    /**
     * 更新某个字段
     * 
     * @param rowKey
     * @param col
     * @param val
     * @throws IOException
     */
    public void update(String rowKey, String col, byte[] val) throws IOException {
        Map<String, byte[]> mp = new HashMap<String, byte[]>();
        mp.put(col, val);
        update(rowKey, mp);
    }

    /**
     * 更新某个ID的数据
     * 
     * @param rowKey
     * @param mp
     * @throws IOException
     */
    public void update(String rowKey, Map<String, byte[]> mp) throws IOException {
        HTableInterface table = null;
        try {
            table = Config.getInstance().getHTable(this.tableName);
            table.setAutoFlush(false);
            Put p = getPut(rowKey, mp);
            table.put(p);
            table.flushCommits();
        } finally {
            if (table != null)
            // release table
            Config.getInstance().pool.putTable(table);
        }
    }

    public void delete(String rowKey) throws IOException {
        HTableInterface table = null;
        try {
            table = Config.getInstance().getHTable(this.tableName);
            table.setAutoFlush(false);
            Delete delete = new Delete(rowKey.getBytes());
            table.delete(delete);
            table.flushCommits();
        } finally {
            if (table != null) Config.getInstance().pool.putTable(table);
        }
    }

    public T get(String rowKey) throws IOException, InstantiationException, IllegalAccessException,
                                   ClassNotFoundException, SecurityException, NoSuchFieldException,
                                   InvocationTargetException, NoSuchMethodException {

        if (StringUtils.isEmpty(rowKey)) {
            return null;
        }
        HTableInterface table = null;
        try {
            table = Config.getInstance().getHTable(this.tableName);

            Get get = new Get(Bytes.toBytes(rowKey));
            get.addFamily(this.family);
            Result result = table.get(get);
            T data = (T) handlerResult(result, this.clazz);
            return data;
        } finally {
            if (table != null) Config.getInstance().pool.putTable(table);
        }
    }

    public byte[] get(String rowKey, String column) throws IOException {
        HTableInterface table = null;
        try {
            table = Config.getInstance().getHTable(this.tableName);

            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(this.family, Bytes.toBytes(column));
            Result result = table.get(get);
            if (result != null && !result.isEmpty()) {
                return result.getValue(this.family, Bytes.toBytes(column));
            }
        } finally {
            if (table != null) Config.getInstance().pool.putTable(table);
        }
        return null;
    }

    public List<T> findAll() throws Exception {
        return findByQuery(null);
    }

    /**
     * rowkey 范围查询
     * 
     * @param startRow
     * @param endRow
     * @return
     * @throws Exception
     */
    public List<T> findByRange(byte[] startRow, byte[] endRow) throws Exception {

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
            List<T> results = new ArrayList<T>();
            for (Result r : rs) {
                T data = (T) handlerResult(r, this.clazz);
                results.add(data);
            }
            return results;
        } finally {
            if (table != null) Config.getInstance().pool.putTable(table);
        }
    }

    /**
     * rowkey 范围查询
     * 
     * @param startRow
     * @param endRow
     * @return
     * @throws Exception
     */
    public List<T> findByRange(byte[] startRow, byte[] endRow, int limit) throws Exception {
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
            scan.setMaxResultSize(limit);
            scan.setFilter(new PageFilter(limit));
            ResultScanner rs = table.getScanner(scan);
            List<T> results = new ArrayList<T>();
            for (Result r : rs) {
                T data = (T) handlerResult(r, this.clazz);
                results.add(data);
            }
            return results;
        } finally {
            if (table != null) Config.getInstance().pool.putTable(table);
        }
    }

    public PageHBase<T> pagination(PageHBase<T> pager) throws Exception {
        int pageSize = pager.getPageSize();
        String nextPageRowkey = pager.getNextPageRowkey();

        HTableInterface table = null;
        try {
            table = Config.getInstance().getHTable(this.tableName);
            Scan scan = new Scan();
            scan.setCaching(100);
            if (StringUtils.isNotEmpty(nextPageRowkey)) {
                scan.setStartRow(Bytes.toBytes(nextPageRowkey));
            }
            scan.setFilter(new PageFilter(pageSize + 1));
            ResultScanner rs = table.getScanner(scan);
            List<T> results = new ArrayList<T>();
            int totalRow = 0;
            for (Result result : rs) {
                totalRow++;
                String rowkey = Bytes.toString(result.getRow());
                if (totalRow == 1) {
                    pager.getPageStartRowMap().put(pager.getCurrentPageNo(), rowkey);
                    pager.setTotalPage(pager.getPageStartRowMap().size());
                }
                if (totalRow > pager.getPageSize()) {
                    pager.setNextPageRowkey(rowkey);
                    pager.setHasNext(true);
                } else {
                    T data = (T) handlerResult(result, this.clazz);
                    results.add(data);
                    pager.setTotalCount(totalRow);
                }
            }
            pager.setResultList(results);
        } finally {
            if (table != null) Config.getInstance().pool.putTable(table);
        }
        return pager;
    }

    public ResultScanner getScanner(HTableInterface table) throws IOException {
        Scan scan = new Scan();
        scan.addFamily(this.family);
        ResultScanner rs = table.getScanner(scan);
        return rs;
    }

    /**
     * 数据组装
     * 
     * @param result
     * @param clazz
     * @return
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws ClassNotFoundException
     * @throws NoSuchFieldException
     * @throws SecurityException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     */
    public T handlerResult(Result result, Class<T> clazz) throws InstantiationException, IllegalAccessException,
                                                         ClassNotFoundException, SecurityException,
                                                         NoSuchFieldException, InvocationTargetException,
                                                         NoSuchMethodException {
        if ((result != null) && (!result.isEmpty())) {
            byte[] rowKey = result.getRow();
            Map<String, HColumnMapping> map = this.mapping.getColumns();
            T data = clazz.newInstance();
            for (String field : map.keySet()) {
                HColumnMapping colMapping = (HColumnMapping) map.get(field);
                byte[] val = result.getValue(this.family, Bytes.toBytes(colMapping.getName()));

                Field f = ClassUtils.getField(data.getClass(), field);
                Object valObj = convertBytes(val, f.getType());
                ObjectUtils.setPropertyValue(data, field, valObj);
            }

            String keyField = this.mapping.getKeyField();
            if (StringUtils.isEmpty(keyField)) return null;
            Field kf = ClassUtils.getField(data.getClass(), keyField);
            Object kValObj = convertBytes(rowKey, kf.getType());
            ObjectUtils.setPropertyValue(data, keyField, kValObj);
            return data;
        }
        return null;

    }

    /**
     * 构造写入操作
     * 
     * @param data
     * @return
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    private Put getPut(T data) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        Object rowKey = PropertyUtils.getProperty(data, mapping.getKeyField());
        Put p = new Put(Bytes.toBytes((String) rowKey));
        Map<String, HColumnMapping> columns = this.mapping.getColumns();
        for (String key : columns.keySet()) {
            HColumnMapping col = columns.get(key);
            Object val = PropertyUtils.getProperty(data, key);
            if (val != null) {
                addByType(p, val, val.getClass(), this.family, col.getName());
            }

        }
        return p;
    }

    /**
     * 构造写入操作
     * 
     * @param rowKey
     * @param datas
     * @return
     */
    protected Put getPut(String rowKey, Map<String, byte[]> datas) {
        Put put = new Put(Bytes.toBytes(rowKey));
        for (String key : datas.keySet()) {
            put.add(this.family, Bytes.toBytes(key), datas.get(key));
        }
        return put;
    }

    /**
     * 添加写入数据
     * 
     * @param put
     * @param val
     * @param type
     * @param family
     * @param qualifier
     */
    private void addByType(Put put, Object val, Class<?> type, byte[] family, String qualifier) {
        Type t = TypeMap.typeOf(type);
        switch (t) {
            case BOOLEAN:
                put.add(family, Bytes.toBytes(qualifier),
                        Bytes.toBytes((Boolean) ConvertUtils.convert(val, Boolean.class)));
                break;
            case INT:
                put.add(family, Bytes.toBytes(qualifier),
                        Bytes.toBytes((Integer) ConvertUtils.convert(val, Integer.class)));
                break;
            case STRING:
                put.add(family, Bytes.toBytes(qualifier), Bytes.toBytes(val + ""));
                break;
            case FLOAT:
                put.add(family, Bytes.toBytes(qualifier), Bytes.toBytes((Float) ConvertUtils.convert(val, Float.class)));
                break;
            case DOUBLE:
                put.add(family, Bytes.toBytes(qualifier),
                        Bytes.toBytes((Double) ConvertUtils.convert(val, Double.class)));
                break;
            case LONG:
                put.add(family, Bytes.toBytes(qualifier), Bytes.toBytes((Long) ConvertUtils.convert(val, Long.class)));
                break;
            case BYTEBUFFER:
                put.add(family, Bytes.toBytes(qualifier), ((ByteBuffer) val).array());
                break;
            default:
                put.add(family, Bytes.toBytes(qualifier), ((ByteBuffer) val).array());
                break;
        }
    }

    /**
     * 转换目标数据类型
     * 
     * @param data
     * @param type
     * @return
     */
    public Object convertBytes(byte[] data, Class<?> type) {
        if (data == null) {
            return null;
        }
        Type t = TypeMap.typeOf(type);
        switch (t) {
            case BOOLEAN:
                return Bytes.toBoolean(data);
            case INT:
                return Bytes.toInt(data);
            case STRING:
                return Bytes.toString(data);
            case FLOAT:
                return Bytes.toFloat(data);
            case DOUBLE:
                return Bytes.toDouble(data);
                // case SHORT:
                // put.add(family, Bytes.toBytes(qualifier),
                // Bytes.toBytes(TypeConvertUtils.(val)));
                // break;
            case LONG:
                return Bytes.toLong(data);
            case BYTEBUFFER:
                return ByteBuffer.wrap(data);
                // case CHAR:
                // put.add(family, Bytes.toBytes(qualifier),
                // Bytes.tobytes);
                // break;
                // case BYTE:
                // put.add
                // break;
            case BIGDECIMAL:
                return Bytes.toBigDecimal(data);
            default:
                return Bytes.toString(data);
        }
    }
}
