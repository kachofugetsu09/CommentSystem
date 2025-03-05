package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.geo.*;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        // 缓存穿透
        // Shop shop = queryPassThrough(id);

        // 使用通用方法解决缓存穿透的问题
        Shop shop = cacheClient.queryPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 互斥锁解决缓存击穿
        // Shop shop = queryWithMutex(id);

        // 逻辑过期解决缓存穿透
        // Shop shop = queryWithLogicalExpire(id);
        // 使用通用方法解决缓存击穿的问题 -> 逻辑过期
//         Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        if(shop==null){
            return Result.fail("店铺信息不存在");
        }
        return Result.ok(shop);
    }

    /**
     * 缓存穿透封装
     * @param id
     * @return
     */
    public Shop queryPassThrough(Long id){
        // 从redis查询缓存
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        if (StrUtil.isNotBlank(shopJson)){
            // 存在
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        if (shopJson != null){
            // 这里就是说redis查询的结果是”“
            return null;
        }
        // 如果不存在需要查询数据库
        Shop shop = getById(id);
        // 如果不存在报错
        if (shop==null){
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 如果存在把数据写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }

    /**
     *  使用互斥锁解决缓存击穿的问题
     * @param id
     * @return
     */
    public Shop queryWithMutex(Long id){
        // 从redis查询缓存
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        if (StrUtil.isNotBlank(shopJson)){
            // 存在，缓存命中
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        if (shopJson != null){
            // redis查询的结果是”“
            return null;
        }
        // 实现缓存重建
        // 获取互斥锁
        Shop shop = null;
        try {
            boolean isLock = tryLock(LOCK_SHOP_KEY + id);
            // 判断是否成功
            if (! isLock) {
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            // 如果不存在需要查询数据库
            shop = getById(id);
            // 模拟延迟
            Thread.sleep(200);
            // 如果不存在报错
            if (shop==null){
                stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            // 如果存在把数据写入redis
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 释放互斥🔒
            unLock(LOCK_SHOP_KEY + id);
        }
        return shop;
    }


    /**
     * 利用redis设置🔒
     * @param key
     * @return
     */
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private  void unLock(String key){
        stringRedisTemplate.delete(key);
    }


    public void saveShop2Redis(Long id,  Long expireSeconds) throws InterruptedException {
        //查询店铺数据
        Shop shop = getById(id);
        Thread.sleep(200);
        // 封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        // 写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id, JSONUtil.toJsonStr(redisData));
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    /**
     * 使用逻辑过期时间解决缓存击穿的问题
     * @param id
     * @return
     */
    public Shop queryWithLogicalExpire(Long id){
        // 从redis查询缓存
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        if (StrUtil.isBlank(shopJson)){
            // 未命中
            return null;
        }
        // 反序列化，得到实例
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 未过期
            return shop;
        }
        // 过期，需要缓存重建
        boolean isLock = tryLock(LOCK_SHOP_KEY + id);
        // 获得互斥锁
        if (isLock){
            // 成功，开启线程进行重建 TODO:使用线程池
            CACHE_REBUILD_EXECUTOR.submit(() ->{
                try {
                    this.saveShop2Redis(id, 20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    // 释放锁
                    unLock(LOCK_SHOP_KEY+id);
                }
            });
        }
        return shop;
    }

    @Override
    @Transactional
    public Result updateShop(Shop shop) {
        Long id = shop.getId();
        if (id == null){
            return Result.fail("店铺id不能为空");
        }
        // 更新是数据库
        updateById(shop);
        // 删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY+id);
        return Result.ok();
    }

}
