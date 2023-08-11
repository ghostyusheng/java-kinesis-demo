package com.gameplus.kinesis.repository;

import com.gameplus.kinesis.config.Config;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.time.Duration;
import java.time.temporal.TemporalUnit;

public class Redis {
    static JedisPool _jedisPool = null;

    public static Jedis getOrInitConn() {
        if (_jedisPool != null) {
            Jedis resource = _jedisPool.getResource();
            return resource;
        }
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxIdle(5);
        poolConfig.setMaxTotal(10);
        poolConfig.setMaxWait(Duration.ofSeconds(5));
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestWhileIdle(true);

        JedisPool jedisPool = new JedisPool(poolConfig, Config.REDIS_URL, Integer.parseInt(Config.REDIS_PORT));
        Jedis jedis;
        jedis = jedisPool.getResource();
        _jedisPool = jedisPool;
        return jedis;
    }
}
