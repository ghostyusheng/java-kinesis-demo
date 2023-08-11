package com.gameplus.kinesis;

import com.gameplus.kinesis.config.Config;
import com.gameplus.kinesis.repository.KafkaConsumer;

import com.gameplus.kinesis.service.Nft;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class App {
    private static final Logger log = LogManager.getLogger(App.class);

    public static void main(String... args) {
        String env = System.getProperty("env", "develop");
        log.info("启动环境为 {}", env);
        if (env == null || !(env.equals("develop") || env.equals("testing") || env.equals("product"))) {
            log.error("请传入正确的环境参数 -Denv= value  ,  develop ｜ testing ｜ product ");
            return;
        }
        log.info("启动程序，使用配置文件 {}.properties", env);
        Config.init(env);
        Nft.initNftCache();
        KafkaConsumer kinesis = new KafkaConsumer(Config.STREAM);
//        kinesis.autoSendTest();
        kinesis.startConsume();
    }
}
