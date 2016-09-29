/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.hbase.mapping;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.BloomType;

import com.lamfire.utils.*;
import com.mob.hbase.anno.*;
import com.mob.hbase.reflection.Utils;

/**
 * @author zxc Aug 30, 2015 7:55:30 PM
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class MappingBuider {

    /**
     * 找到对应包映射
     * 
     * @param pkg
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static HBaseStore buildPkg(String pkg) throws IOException, ClassNotFoundException {
        HBaseStore store = HBaseStore.getNewStore();
        Set<Class<?>> set = ClassLoaderUtils.getClasses(pkg);
        for (Class clzz : set) {
            if (!clzz.isAnnotationPresent(ColumnFamily.class)) {
                continue;
            }
            ColumnFamily cf = (ColumnFamily) clzz.getAnnotation(ColumnFamily.class);
            Map<String, Field> fieldMap = ClassUtils.getAllFieldsAsMap(clzz);
            Map columns = new HashMap();
            String keyField = null;
            for (String key : fieldMap.keySet()) {
                Field f = (Field) fieldMap.get(key);
                if (f.isAnnotationPresent(Column.class)) {
                    Column cm = (Column) f.getAnnotation(Column.class);
                    String name = StringUtils.isEmpty(cm.name()) ? f.getName() : cm.name();
                    HColumnMapping hcm = new HColumnMapping(name);
                    columns.put(f.getName(), hcm);
                }
                if (f.isAnnotationPresent(RowKey.class)) {
                    keyField = f.getName();
                }
            }
            HColumnFamilyMapping cmf = mappingFamily(cf, columns, keyField);
            store.getOrCreateTable(cmf.getTableName()).addColumnFamily(cmf);
        }
        return store;
    }

    /**
     * 根据类创建columnMapping
     * 
     * @param clzz
     * @return
     */
    public static HColumnFamilyMapping getHColumnMapping(Class<?> clzz) {
        if (!clzz.isAnnotationPresent(ColumnFamily.class)) {
            return null;
        }
        ColumnFamily cf = clzz.getAnnotation(ColumnFamily.class);

        Map<String, Field> fieldMap = Utils.getAllFields(clzz);
        Map<String, HColumnMapping> columns = new HashMap<String, HColumnMapping>();
        String keyField = null;
        for (String key : fieldMap.keySet()) {
            Field f = fieldMap.get(key);
            if (f.isAnnotationPresent(Column.class)) {
                Column cm = f.getAnnotation(Column.class);
                String name = StringUtils.isEmpty(cm.name()) ? f.getName() : cm.name();
                HColumnMapping hcm = new HColumnMapping(name);
                columns.put(f.getName(), hcm);
            }
            if (f.isAnnotationPresent(RowKey.class)) {
                keyField = f.getName();
            }
        }
        HColumnFamilyMapping cmf = mappingFamily(cf, columns, keyField);

        return cmf;
    }

    private static HColumnFamilyMapping mappingFamily(ColumnFamily cf, Map<String, HColumnMapping> columns,
                                                      String keyField) {

        String familyName = cf.familyName();
        String tableName = cf.tableName();
        // String qualifier = cf.qualifier();
        Algorithm compression = cf.compression();
        boolean blockCache = cf.blockCache();
        int blockSize = cf.blockSize();
        BloomType bloomFilter = cf.bloomFilter();
        int maxVersions = cf.maxVersions();
        int timeToLive = cf.timeToLive();
        boolean inMemory = cf.inMemory();
        HColumnFamilyMapping cm = new HColumnFamilyMapping(familyName, tableName, compression, blockCache, blockSize,
                                                           bloomFilter, maxVersions, timeToLive, inMemory);
        cm.addColumns(columns);
        cm.setKeyField(keyField);
        return cm;
    }
}
