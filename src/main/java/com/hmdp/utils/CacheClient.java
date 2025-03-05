package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import sun.security.krb5.internal.PAData;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

/**
 * 作者：24岁没牵过女人的手
 * 日期：2023/3/8 13:16
 * 文件描述：
 */

@Slf4j
@Component
public class CacheClient {
    @Autowired
    private final StringRedisTemplate stringRedisTemplate;


    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        // 设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        // 写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /**
     * 使用null值解决redis缓存击穿的方法
     * @param keyPrefix
     * @param id
     * @param type
     * @param dbFallBack
     * @param time
     * @param unit
     * @return
     * @param <R>
     * @param <PARAM>
     */
    public <R, PARAM> R queryPassThrough(
            String keyPrefix, PARAM id, Class<R> type, Function<PARAM, R> dbFallBack,  Long time, TimeUnit unit){
        // 拼接key
        String key = keyPrefix+id;
        // 从redis查询缓存
        String jsonStr = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(jsonStr)){
            // 存在
            return JSONUtil.toBean(jsonStr, type);
        }
        if (jsonStr != null){
            // 这里就是说redis查询的结果是”“
            return null;
        }
        // 如果不存在需要查询数据库
        R r = dbFallBack.apply(id);
        // 如果不存在报错
        if (r==null){
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 如果存在把数据写入redis
        this.set(key, r, time, unit);
        return r;
    }

    /**
     * 获取🔒
     * @param key
     * @return
     */
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 释放🔒
     * @param key
     */
    private  void unLock(String key){
        stringRedisTemplate.delete(key);
    }


    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);


    public <R, PARAM> R queryWithLogicalExpire(String keyPrefix, PARAM param, Class<R> type, Function<PARAM, R> function,   Long time, TimeUnit unit){
        // 从redis查询缓存
        String shopJson = stringRedisTemplate.opsForValue().get(keyPrefix + param);
        if (StrUtil.isBlank(shopJson)){
            // 未命中
            return null;
        }
        // 反序列化，得到实例
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 未过期
            return r;
        }
        // 过期，需要缓存重建
        boolean isLock = tryLock(LOCK_SHOP_KEY + param);
        // 获得互斥锁
        if (isLock){
            // 成功，开启线程进行重建 TODO:使用线程池
            CACHE_REBUILD_EXECUTOR.submit(() ->{
                try {
                    // 查询是数据库
                    R r1 = function.apply(param);
                    // 写入redis
                    this.setWithLogicalExpire(LOCK_SHOP_KEY + param, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    // 释放锁
                    unLock(LOCK_SHOP_KEY+param);
                }
            });
        }
        return r;
    }


}
