/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.hbase.config;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.*;

import com.lamfire.logger.Logger;
import com.mob.hbase.mapping.*;

/**
 * Hbase 配置信息
 * 
 * @author zxc Aug 28, 2015 11:46:11 AM
 */
@SuppressWarnings("deprecation")
public class Config {

    static final Logger    logger = Logger.getLogger(Config.class);

    private HBaseAdmin     hbaseAdmin;
    private Configuration  conf;                                   // hbase 配置文件
    private Properties     hbase_init;                             // 程序 hbase 配置文件
    private Properties     hbase_mapping;                          // hbase
    private static boolean init   = false;
    private static Config  config = null;

    public HBaseStore      store;                                  // hbase 数据结构模型仓库

    public HTablePool      pool;                                   // hbase table pool

    private Config() {
    }

    public static void init() {
        if (!init) {
            try {
                config = new Config();
                Configuration conf = HBaseConfiguration.create();
                Properties p = getCfg();
                for (Object key : p.keySet()) {
                    conf.set((String) key, p.getProperty((String) key));
                }
                HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
                config.setHbase_init(p);
                Properties mp = getMapping();
                String pkg = (String) mp.get("hbase.cfg.mapping.pkg");
                HBaseStore store = MappingBuider.buildPkg(pkg);
                config.setStore(store);
                config.setHbase_mapping(mp);
                config.setConf(conf);
                config.setHbaseAdmin(hbaseAdmin);
                init = true;
            } catch (Exception e) {
                logger.error("[Config init]", e);
            }
        }
    }

    public static Config getInstance() {
        init();
        return config;
    }

    /**
     * 创建连接池
     * 
     * @return
     */
    @SuppressWarnings("resource")
    public static HTablePool buildPool() {
        Config c = getInstance();
        Properties props = c.getHbase_mapping();
        String val = props.getProperty("hbase.cfg.table.poolsize", "20");
        HTablePool pool = new HTablePool(c.getConf(), Integer.valueOf(val));
        c.pool = pool;
        return c.pool;
    }

    /**
     * 获取htable
     * 
     * @param tableName
     * @return
     */
    public HTableInterface getHTable(String tableName) {
        if (config.getPool() == null) {
            Config.buildPool();
        }
        HTableInterface hface = config.getPool().getTable(tableName);
        return hface;
    }

    public static void buildMapping() throws IOException, ClassNotFoundException {
        getInstance();
        Properties props = Config.getInstance().getHbase_mapping();
        if ("true".equals(props.get("hbase.cfg.table.build"))) {
            String pkg = (String) props.get("hbase.cfg.mapping.pkg");
            HBaseStore store = MappingBuider.buildPkg(pkg);
            store.buildAll(true);
        }
    }

    /**
     * 创建列族
     * 
     * @param clzz
     * @param tbName
     * @throws IOException
     */
    public static void addColumnFamily(Class<?> clzz, String tbName) throws IOException {
        HColumnFamilyMapping mp = MappingBuider.getHColumnMapping(clzz);
        if (mp != null) {
            HColumnDescriptor hColumnDesc = mp.build();
            getInstance().getHbaseAdmin().addColumn(tbName, hColumnDesc);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {

        Config.getInstance();
        Config.addColumnFamily(Class.forName("com"), "");

        // admin.addColumn(tableName, column)

        // Config.getInstance().buildMapping();
        // System.out.println();

        // Properties props = Config.getInstance().getHbase_mapping();
        // String pkg = (String)props.get("hbase.cfg.mapping.pkg");
        // HBaseStore store = MappingBuider.buildPkg(pkg);
        // store.build("CF_PHONES", true);

        // HColumnDescriptor hd = new HColumnDescriptor("CF_PHONES");
        // Config.getInstance().getHbaseAdmin().addColumn("DEVICE", hd);
        // System.out.println("e");

        // Config.getInstance().getHbaseAdmin().deleteTable(tableName)
    }

    private static Properties getCfg() throws IOException {
        Properties config = new Properties();
        config.setProperty("hbase.zookeeper.property.clientPort", "2222");
        config.setProperty("hbase.zookeeper.quorum", "192.168.10.182,192.168.10.183,192.168.10.184");
        config.setProperty("hbase.regionserver.msginterval", "1000");
        config.setProperty("hbase.client.pause", "5000");
        config.setProperty("hbase.master.meta.thread.rescanfrequency", "10000");
        config.setProperty("hbase.server.thread.wakefrequency", "1000");
        config.setProperty("hbase.regionserver.handler.count", "5");
        config.setProperty("hbase.master.lease.period", "6000");
        config.setProperty("hbase.master.info.port", "-1");
        config.setProperty("hbase.regionserver.info.port", "-1");
        config.setProperty("hbase.regionserver.info.port.auto", "true");
        config.setProperty("hbase.master.lease.thread.wakefrequency", "3000");
        config.setProperty("hbase.regionserver.optionalcacheflushinterval", "10000");
        config.setProperty("hbase.regionserver.safemode", "false");
        config.setProperty("hbase.hregion.max.filesize", "67108864");
        config.setProperty("hadoop.log.dir", "${user.dir}/../logs");
        return config;
    }

    private static Properties getMapping() throws IOException {
        Properties config = new Properties();
        config.setProperty("hbase.cfg.mapping.pkg", "com.mob.sms.data.hbase.entity");
        config.setProperty("hbase.cfg.table.build", "true");
        config.setProperty("hbase.cfg.table.poolsize", "100");
        return config;
    }

    public HBaseAdmin getHbaseAdmin() {
        return hbaseAdmin;
    }

    public void setHbaseAdmin(HBaseAdmin hbaseAdmin) {
        this.hbaseAdmin = hbaseAdmin;
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Properties getHbase_init() {
        return hbase_init;
    }

    public void setHbase_init(Properties hbase_init) {
        this.hbase_init = hbase_init;
    }

    public Properties getHbase_mapping() {
        return hbase_mapping;
    }

    public void setHbase_mapping(Properties hbase_mapping) {
        this.hbase_mapping = hbase_mapping;
    }

    public HBaseStore getStore() {
        return store;
    }

    public void setStore(HBaseStore store) {
        this.store = store;
    }

    public HTablePool getPool() {
        return pool;
    }
}
