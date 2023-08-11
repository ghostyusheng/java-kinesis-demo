package com.gameplus.kinesis.repository;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.gameplus.kinesis.stream.MarketOrder;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Properties;

import com.gameplus.kinesis.utils.PropertyUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Redshift {
    private static final Logger log = LogManager.getLogger(MarketOrder.class);
    public static Redshift instance;
    public static DataSource pool;

    public static Connection getOrInitConn() throws Exception {
        if (Redshift.pool != null) {
            return Redshift.pool.getConnection();
        }

        Properties config = PropertyUtil.getPropertiesByPrefix("druid");

//        ResourceBundle druid = ResourceBundle.getBundle("druid");
//        DruidDataSource dSource = null;
//        Properties dProperties = convertResourceBundleToProperties(druid);
//        Enumeration<Object> keys = dProperties.keys();
//        while (keys.hasMoreElements()) {
//            String key = (String) keys.nextElement();
//            String value = (String) dProperties.get(key);
//            log.info(key + " = " + value);
//        }

        DruidDataSource dataSource = (DruidDataSource) DruidDataSourceFactory.createDataSource(config);
        Connection conn = dataSource.getConnection();
        Redshift.pool = dataSource;
        return conn;
    }
}
