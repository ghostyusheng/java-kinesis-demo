package com.gameplus.kinesis.config;

import java.util.ResourceBundle;

public final class Config {
    public static String ES_HOST;
    public static String ES_PORT;
    public static String STREAM;
    public static String APPLICATION;
    public static String REGION;
    public static String REDSHIFT_URL;
    public static String REDSHIFT_USER;
    public static String REDSHIFT_PASS;
    public static String REDIS_URL;
    public static String REDIS_PORT;

    public static String ENV;

    public static final String NFT_REDIS_NFT_LABEL = "bd:market:nft_label:";

    // nft 属性  key:value
    public static final String NFT_REDIS_NFT_ATTR = "bd:market:nft_attr:";

    // 记录 nft 的属性 keys 集合
    public static final String NFT_REDIS_NFT_ATTR_SET = "bd:market:nft_attr_set:";


    public static void init(String env) {
        ENV = env;
        ResourceBundle resource = ResourceBundle.getBundle(ENV);
        Config.ES_HOST = resource.getString("ES_HOST");
        Config.ES_PORT = resource.getString("ES_PORT");
        Config.STREAM = resource.getString("STREAM");
        Config.APPLICATION = resource.getString("APPLICATION");
        Config.REGION = resource.getString("REGION");
        Config.REDSHIFT_URL = resource.getString("REDSHIFT_URL");
        Config.REDSHIFT_USER = resource.getString("REDSHIFT_USER");
        Config.REDSHIFT_PASS = resource.getString("REDSHIFT_PASS");
        Config.REDIS_URL = resource.getString("REDIS_URL");
        Config.REDIS_PORT = resource.getString("REDIS_PORT");
    }

}
