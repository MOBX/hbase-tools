/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.hbase.mapping;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.HTableDescriptor;

import com.mob.hbase.config.Config;

/**
 * @author zxc Aug 28, 2015 11:46:11 AM
 */
public class HBaseStore {

    public Map<String, HTableMapping> store = new HashMap<String, HTableMapping>();

    private HBaseStore() {
    }

    public static HBaseStore getNewStore() {
        return new HBaseStore();
    }

    public HTableMapping getOrCreateTable(String tableName) {
        if (!store.containsKey(tableName)) {
            HTableMapping htb = new HTableMapping(tableName);
            store.put(tableName, htb);
        }
        return store.get(tableName);
    }

    public void put(String key, HTableMapping mapping) {
        store.put(key, mapping);
    }

    public HTableMapping get(String tableName) {
        return store.get(tableName);
    }

    public void buildAll(boolean force) throws IOException {
        for (String key : store.keySet()) {
            build(key, force);
        }

    }

    public void build(String tableName, boolean force) throws IOException {
        HTableMapping tableMapping = store.get(tableName);
        HTableDescriptor descriptor = tableMapping.build();
        Config cfg = Config.getInstance();

        if (cfg.getHbaseAdmin().tableExists(tableName)) {
            if (force) {
                cfg.getHbaseAdmin().disableTable(tableName);
                cfg.getHbaseAdmin().deleteTable(tableName);
                cfg.getHbaseAdmin().createTable(descriptor);
                // cfg.getHbaseAdmin().add
            }
        } else {
            cfg.getHbaseAdmin().createTable(descriptor);
        }
    }

}
