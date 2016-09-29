/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.hbase.anno;

import java.lang.annotation.*;

import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.BloomType;

/**
 * hbase 列族
 * 
 * @author zxc Aug 28, 2015 11:46:11 AM
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ java.lang.annotation.ElementType.TYPE })
public @interface ColumnFamily {

    public abstract String familyName(); // 列族名称

    public abstract String tableName(); // 所属表

    // public abstract String qualifier();

    public abstract Algorithm compression() default Algorithm.NONE; // 是否压缩

    public abstract boolean blockCache() default false; // 块缓存

    public abstract int blockSize() default -1; // 块缓存大小

    public abstract BloomType bloomFilter() default BloomType.NONE; // 有布隆过滤

    public abstract int maxVersions() default -1;

    public abstract int timeToLive() default -1;

    public abstract boolean inMemory() default false; // 存放内存
}
