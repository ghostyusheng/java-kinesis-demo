package com.gameplus.kinesis.service;

import com.gameplus.kinesis.config.Config;
import com.gameplus.kinesis.repository.Redis;
import com.gameplus.kinesis.repository.Redshift;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class Nft {

    private static final Logger log = LogManager.getLogger(Nft.class);

    private static ScheduledExecutorService scheduledThreadPool = Executors.newScheduledThreadPool(2, new ThreadFactory() {
        private int i = 0;

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "refreshNft" + i++);
        }
    });


    /**
     * 程序初始化时 ，将 market.nft 信息加载到 redis
     */
    public static void initNftCache() {
        new SyncNftTask().run();
        new SyncNftAttrTask().run();
        scheduledThreadPool.scheduleWithFixedDelay(new SyncNftTask(), 0, 60, TimeUnit.MINUTES);
        scheduledThreadPool.scheduleWithFixedDelay(new SyncNftAttrTask(), 0, 10, TimeUnit.MINUTES);
    }


    private static class SyncNftTask implements Runnable {
        private static final Logger log = LogManager.getLogger(SyncNftTask.class);

        @Override
        public void run() {
            int i = 0;
            Jedis redis = null;
            try {
                redis = Redis.getOrInitConn();
                Connection conn = Redshift.getOrInitConn();
                Statement stmt = conn.createStatement();
                String sql = "select nft_id,nft_label_ids from market.nft";
                ResultSet rs = stmt.executeQuery(sql);
                while (rs.next()) {
                    redis.set(Config.NFT_REDIS_NFT_LABEL + rs.getString("nft_id"), rs.getString("nft_label_ids"));
                    i++;
                }
                if (rs != null) rs.close();
                if (stmt != null) stmt.close();
                if (conn != null) conn.close();
                if (redis != null) redis.close();
                log.info("刷新一次 nft to redis success , 数据条数 {}", i);

            } catch (Exception e) {
                log.error("刷新 nft to redis fail , 数据条数 {} , e = {} ", i, e);
            } finally {
                if (redis != null)
                    redis.close();
            }


        }
    }


    /**
     * 将 nft 的 attr 信息 以 hahsmap 结构存到 redis
     * 1 . redis hash 只能存 string 类型  ，数值信息丢失了
     * 2 . 更新的问题
     */
    private static class SyncNftAttrTask implements Runnable {
        private static final Logger log = LogManager.getLogger(SyncNftAttrTask.class);

        @Override
        public void run() {
            int i = 0;

            /**
             * nft 属性 更新问题  。属性删除
             */
            HashMap<String, Boolean> nftFlagMap = new HashMap<>();

            Jedis redis = null;
            try {
                redis = Redis.getOrInitConn();
                Connection conn = Redshift.getOrInitConn();
                Statement stmt = conn.createStatement();
                String sql = "select nft_id , attr , value , type from market.t_nft_attribute";
                ResultSet rs = stmt.executeQuery(sql);

                while (rs.next()) {

                    if (nftFlagMap.get(rs.getString("nft_id")) == null || nftFlagMap.get(rs.getString("nft_id")).booleanValue()) {
                        // 第一次进来 清空 当前 nft 的属性 set
                        redis.del(Config.NFT_REDIS_NFT_ATTR_SET + rs.getString("nft_id"));
                        // 记录 nft 有哪些属性 名称
                        redis.sadd(Config.NFT_REDIS_NFT_ATTR_SET + rs.getString("nft_id"), rs.getString("attr"));
                        nftFlagMap.put(rs.getString("nft_id"), false);
                    } else {
                        redis.sadd(Config.NFT_REDIS_NFT_ATTR_SET + rs.getString("nft_id"), rs.getString("attr"));
                    }
                    // type        tinyint   default 0                 not null comment '属性值真实类型(0:字符串,1:数值)'
                    // 记录属性类型 和 属性值
                    String typeAndValue = rs.getInt("type") + "@#" + rs.getString("value");
                    redis.set(Config.NFT_REDIS_NFT_ATTR + rs.getString("nft_id") + rs.getString("attr"), typeAndValue);
                    i++;
                }
                if (rs != null) rs.close();
                if (stmt != null) stmt.close();
                if (conn != null) conn.close();
                if (redis != null) redis.close();
                log.info("刷新一次 nft attr to redis success , 数据条数 {}", i);

            } catch (Exception e) {
                log.error("刷新 nft attr to redis fail , 数据条数 {} , e = {} ", i, e);
            } finally {
                if (redis != null)
                    redis.close();
            }

        }
    }


    public static String getNftLabelIdsByNftId(String nft_id) {
        if (nft_id == null || nft_id == "") {
            return null;
        }
        Jedis redis = Redis.getOrInitConn();
        String s = redis.get(Config.NFT_REDIS_NFT_LABEL + nft_id);
        if (redis != null) redis.close();

        return s;
    }


    public static Map getNftAttrsByNftId(String nft_id) {
        Map<String, Object> res = new HashMap<>();
        if (nft_id == null || nft_id == "") {
            return res;
        }
        Jedis redis = Redis.getOrInitConn();
        // 记录有哪些 attr
        Set<String> attrKeys = redis.smembers(Config.NFT_REDIS_NFT_ATTR_SET + nft_id);
        for (String ak : attrKeys) {
            // type        tinyint   default 0                 not null comment '属性值真实类型(0:字符串,1:数值)'
            // 记录属性类型 和 属性值
            String typeAndValue = redis.get(Config.NFT_REDIS_NFT_ATTR + nft_id + ak);
            if (typeAndValue == null || typeAndValue.trim().equals("") || typeAndValue.split("@#").length != 2) {
                log.error("nft attrs 信息有误 nft_id = {} , typeAndValue = {}", nft_id, typeAndValue);
                continue;
            }
            String[] arr = typeAndValue.split("@#");
            String val = arr[1];
            if (arr[0].equals("0")) {
                // 0 字符串
                res.put(ak, val);
            } else {
                // 数值
                double valDouble = Double.valueOf(val).doubleValue();
                res.put(ak, valDouble);
            }
        }
        if (redis != null) redis.close();
        return res;
    }
}
