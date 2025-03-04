package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 查询店铺类型列表
     */
    @Override
    public List<ShopType> queryTypeList() {
        String key = RedisConstants.CACHE_SHOP_TYPE_KEY;

        // 1. 查询缓存
        List<String> result = stringRedisTemplate.opsForList().range(key, 0, -1);
        if (result != null && !result.isEmpty()) {
            // 缓存命中，返回解析后的数据
            return result.stream()
                    .map(s -> JSONUtil.toBean(s, ShopType.class))
                    .collect(Collectors.toList());
        }

        String emptyMarker = stringRedisTemplate.opsForValue().get(key);
        String empty = "";
        if (empty.equals(emptyMarker)) {
            return Collections.emptyList();
        }

        List<ShopType> list = query().orderByAsc("sort").list();

        if (list == null || list.isEmpty()) {
            stringRedisTemplate.opsForValue().set(key, empty, RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            return Collections.emptyList();
        }

        for (ShopType shopType : list) {
            String json = JSONUtil.toJsonStr(shopType);
            stringRedisTemplate.opsForList().rightPush(key, json);
        }

        stringRedisTemplate.expire(key, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);

        return list;
    }
}

