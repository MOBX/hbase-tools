/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.hbase.mapping;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * 数据类型映射
 * 
 * @author zxc Aug 28, 2015 11:46:11 AM
 */
public class TypeMap {

    @SuppressWarnings("rawtypes")
    public static Map<Class, Type> typeMap = new HashMap<Class, Type>();

    static {
        typeMap.put(Boolean.class, Type.BOOLEAN);
        typeMap.put(boolean.class, Type.BOOLEAN);

        typeMap.put(Integer.class, Type.INT);
        typeMap.put(int.class, Type.INT);

        typeMap.put(Long.class, Type.LONG);
        typeMap.put(long.class, Type.LONG);

        typeMap.put(Float.class, Type.FLOAT);
        typeMap.put(float.class, Type.FLOAT);

        typeMap.put(Double.class, Type.DOUBLE);
        typeMap.put(double.class, Type.DOUBLE);

        // typeMap.put(Short.class, Type.SHORT);
        // typeMap.put(short.class, Type.SHORT);

        typeMap.put(String.class, Type.STRING);

        // typeMap.put(Byte.class, Type.BYTE);
        // typeMap.put(byte.class, Type.BYTE);

        // typeMap.put(char.class, Type.CHAR);
        typeMap.put(BigDecimal.class, Type.BIGDECIMAL);

        typeMap.put(ByteBuffer.class, Type.BYTEBUFFER);
    }

    public static Type typeOf(Class<?> clazz) {
        return typeMap.get(clazz);
    }
}
