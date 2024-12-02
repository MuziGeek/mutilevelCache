package com.muzi.mutievelcache.core;

import com.github.benmanes.caffeine.cache.Cache;
import com.muzi.mutievelcache.properties.CacheConfigProperties;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.support.AbstractValueAdaptingCache;
import org.springframework.cache.support.NullValue;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
@Slf4j
public class RedisCaffeineCahe extends AbstractValueAdaptingCache {
    @Getter
    private final String name;

    @Getter
    private final Cache<Object, Object> caffeineCache;

    private final RedisTemplate<Object, Object> stringKeyRedisTemplate;

    private final String cachePrefix;

    private final Duration defaultExpiration;

    private final Duration defaultNullValuesExpiration;

    private final Map<String, Duration> expires;

    private final String topic;

    private final Map<String, ReentrantLock> keyLockMap = new ConcurrentHashMap<>();

    private RedisSerializer<String> stringSerializer = RedisSerializer.string();

    private RedisSerializer<Object> javaSerializer = RedisSerializer.java();

    protected RedisCaffeineCahe(boolean allowNullValues) {
        super(allowNullValues);
    }
    public RedisCaffeineCache(String name, RedisTemplate<Object, Object> stringKeyRedisTemplate,
                              Cache<Object, Object> caffeineCache, CacheConfigProperties cacheConfigProperties) {
        super(cacheConfigProperties.isCacheNullValues());
        this.name = name;
        this.stringKeyRedisTemplate = stringKeyRedisTemplate;
        this.caffeineCache = caffeineCache;
        this.cachePrefix = cacheConfigProperties.getCachePrefix();
        this.defaultExpiration = cacheConfigProperties.getRedis().getDefaultExpiration();
        this.defaultNullValuesExpiration = cacheConfigProperties.getRedis().getDefaultNullValuesExpiration();
        this.expires = cacheConfigProperties.getRedis().getExpires();
        this.topic = cacheConfigProperties.getRedis().getTopic();
    }

    @Override
    //子类获取缓存主要实现的逻辑方法
    protected Object lookup(Object key) {
        //组装key
        Object cacheKey = getKey(key);
        //如果key不存在，直接返回null
        Object value = caffeineCache.getIfPresent(key);
        if (value != null) {
            log.debug("get cache from caffeine, the key is : {}", cacheKey);
            return value;
        }
        //查询二级缓存，如果存在，则先放入一级缓存中
        value = getRedisValue(key);
        if (value != null) {
            log.debug("get cache from redis and put in caffeine, the key is : {}", cacheKey);
            caffeineCache.put(key, value);
        }
        return value;
    }


    @Override
    public Object getNativeCache() {
        return this;
    }

    @Override
    public <T> T get(Object o, Callable<T> callable) {
    }

    @Override
    public void put(Object o, Object o1) {

    }

    @Override
    public ValueWrapper putIfAbsent(Object key, Object value) {
        return null;
    }

    @Override
    public void evict(Object o) {

    }

    @Override
    public boolean evictIfPresent(Object key) {
        return false;
    }

    @Override
    public void clear() {

    }

    @Override
    public boolean invalidate() {
        return false;
    }


    private Object getKey(Object key) {
        return this.name.concat(":").concat(
                StringUtils.isEmpty(cachePrefix) ? key.toString() : cachePrefix.concat(":").concat(key.toString()));
    }

    private Duration getExpire(Object value) {
        Duration cacheNameExpire = expires.get(this.name);
        if (cacheNameExpire == null) {
            cacheNameExpire = defaultExpiration;
        }
        if ((value == null || value == NullValue.INSTANCE) && this.defaultNullValuesExpiration != null) {
            cacheNameExpire = this.defaultNullValuesExpiration;
        }
        return cacheNameExpire;
    }

    /**
     * @param message
     * @description 缓存变更时通知其他节点清理本地缓存
     * @author muzi
     *
     */
    private void push(CacheMessage message) {

        /**
         * 为了能自定义redisTemplate，发布订阅的序列化方式固定为jdk序列化方式。
         */
        Assert.hasText(topic, "a non-empty channel is required");
        byte[] rawChannel = stringSerializer.serialize(topic);
        byte[] rawMessage = javaSerializer.serialize(message);
        stringKeyRedisTemplate.execute((connection) -> {
            connection.publish(rawChannel, rawMessage);
            return null;
        }, true);

        // stringKeyRedisTemplate.convertAndSend(topic, message);
    }

    /**
     * @param key
     * @description 清理本地缓存
     * @author fuwei.deng
     * @date 2018年1月31日 下午3:15:39
     * @version 1.0.0
     */
    public void clearLocal(Object key) {
        log.debug("clear local cache, the key is : {}", key);
        if (key == null) {
            caffeineCache.invalidateAll();
        }
        else {
            caffeineCache.invalidate(key);
        }
    }

    private void setRedisValue(Object key, Object value, Duration expire) {

        Object convertValue = value;
        if (value == null || value == NullValue.INSTANCE) {
            convertValue = RedisNullValue.REDISNULLVALUE;
        }

        if (!expire.isNegative() && !expire.isZero()) {
            stringKeyRedisTemplate.opsForValue().set(getKey(key), convertValue, expire);
        }
        else {
            stringKeyRedisTemplate.opsForValue().set(getKey(key), convertValue);
        }
    }

    private Object getRedisValue(Object key) {

        // NullValue在不同序列化方式中存在问题，因此自定义了RedisNullValue做个转化。
        Object value = stringKeyRedisTemplate.opsForValue().get(getKey(key));
        if (value != null && value instanceof RedisNullValue) {
            value = NullValue.INSTANCE;
        }
        return value;
    }
}
