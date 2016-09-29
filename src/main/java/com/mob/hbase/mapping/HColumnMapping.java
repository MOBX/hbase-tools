/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.hbase.mapping;

/**
 * 列名
 * 
 * @author zxc Aug 28, 2015 11:46:11 AM
 */
public class HColumnMapping {

    private String name;

    public HColumnMapping(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
