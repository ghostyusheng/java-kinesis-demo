package com.gameplus.kinesis.utils;

import com.gameplus.kinesis.config.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Enumeration;
import java.util.Properties;
import java.util.ResourceBundle;

public class PropertyUtil {
    private static final Logger log = LogManager.getLogger(PropertyUtil.class);

    public static Properties convertResourceBundleToProperties(ResourceBundle resource) {
        Properties properties = new Properties();
        Enumeration<String> keys = resource.getKeys();
        while (keys.hasMoreElements()) {
            String key = keys.nextElement();
            properties.put(key, resource.getString(key));
        }
        return properties;
    }

    /**
     * 获取指定前缀的 配置属性
     *
     * @param prefix
     * @return
     */
    public static Properties getPropertiesByPrefix(String prefix) {
        if (prefix == null || prefix.trim().equals("")) {
            log.error("非法的配置前缀 {}", prefix);
            throw new RuntimeException("非法的配置前缀 " + prefix);
        }
        ResourceBundle resourceBundle = ResourceBundle.getBundle(Config.ENV);
        Properties properties = convertResourceBundleToProperties(resourceBundle);
        Properties result = new Properties();
        for (Object k : properties.keySet()) {
            String key = (String) k;
            if (key.startsWith(prefix)) {
                // 从 kafka.bootstrap.servers 到 bootstrap.servers
                result.put(key.substring(prefix.length() + 1), properties.getProperty(key));
            }
        }
        return result;
    }
}
