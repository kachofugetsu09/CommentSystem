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
     * @return
     */
    @Override
    public List<ShopType> queryTypeList() {
        String key = RedisConstants.CACHE_SHOP_TYPE_KEY;
        List<String> result = stringRedisTemplate.opsForList().range(key, 0, -1);
        if (result != null && !result.isEmpty()) {
                return result.stream().map(s ->
                        JSONUtil.toBean(s, ShopType.class)).collect(Collectors.toList());
            }
            List<ShopType> list = query().orderByAsc("sort").list();
            if (list == null) {
                return Collections.emptyList();
            }
            for (ShopType shopType : list) {
                String json = JSONUtil.toJsonStr(shopType);
                stringRedisTemplate.opsForList().rightPush(key, json);
            }
            return list;
        }
    }

