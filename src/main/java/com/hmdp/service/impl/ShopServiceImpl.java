package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSON;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
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
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;


    //解决缓存穿透的方案：查询不存在的数据，对于给定的key设置空值放到Redis,避免大量请求到达数据库
    @Override
    public Result querById(Long id) {
        Shop shop=cacheClient.queryWithPassThrough(CACHE_SHOP_KEY,id,Shop.class,this::getById,CACHE_SHOP_TTL,TimeUnit.MINUTES);
        if (shop==null)
        {
            return Result.fail("店铺不存在!");
        }
        return Result.ok(shop);
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id=shop.getId();
        if (id==null)
        {
            return Result.fail("店铺ID不能为空");
        }
        //更新数据库
        updateById(shop);
        //删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY+id);
        return Result.ok();
    }

    public Shop queryWithMutex(Long id)
    {
        String shopJson=stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY+id);
        //判断是否存在
        if (StrUtil.isNotBlank(shopJson))
        {
            //存在则直接返回
            return JSONUtil.toBean(shopJson,Shop.class);
        }
        //判断是否为空值
        if (shopJson!=null)
        {
            return null;
        }
        //实现缓存重建
        //获取互斥锁
        String lockKey=LOCK_SHOP_KEY+id;
        Shop shop=null;
        try {
            boolean isLock=tryLock(lockKey);
            //判断是否获取成功
            if (!isLock)
            {
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            //失败，休眠并重试
            //成功，则根据id查数据库
            shop=getById(id);
            if (shop==null)
            {
                //将空值写入redis, 解决缓存穿透问题
                stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
                return null;
            }
            //存在则写入redis
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        }catch(InterruptedException e)
        {
            throw new RuntimeException(e);
        }finally {
            unLock(lockKey);
        }
        //返回
        return shop;
    }

    public Shop queryWithPassThrough(Long id)
    {
        String shopJson=stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY+id);
        //判断是否存在
        if (StrUtil.isNotBlank(shopJson))
        {
            //存在则直接返回
            return JSONUtil.toBean(shopJson,Shop.class);
        }
        if (shopJson!=null)
        {
            return null;
        }
        //不存在，根据id查询数据库
        Shop shop=getById(id);
        if (shop==null)
        {
            //将空值写入redis, 解决缓存穿透问题
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
        //存在则写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //返回
        return shop;
    }

    private boolean tryLock(String key)
    {
        Boolean flag=stringRedisTemplate.opsForValue().setIfAbsent(key,"1",10,TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unLock(String key)
    {
        stringRedisTemplate.delete(key);
    }

    //在缓存击穿的逻辑时间方案中查找数据库并重新构建缓存
    private void saveShop2Redis(Long id,Long expireSeconds)
    {
        Shop shop=getById(id);
        RedisData redisData=new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(redisData));
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR= Executors.newFixedThreadPool(10);
    public Shop queryWithLogicalExpire(Long id)
    {
        String shopJson=stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY+id);
        //判断是否存在
        if (StrUtil.isBlank(shopJson))
        {
            //存在则直接返回
            return null;
        }
        //命中，判断过期时间，需要先把json反序列化为对象
        RedisData redisData=JSONUtil.toBean(shopJson,RedisData.class);
        Shop shop=JSONUtil.toBean((JSONObject) redisData.getData(),Shop.class);
        LocalDateTime expireTime=redisData.getExpireTime();
        //判断是否过期
        if (expireTime.isAfter(LocalDateTime.now()))
        {
            return shop;
        }
        //已过期，则需要缓存重建
        String lockKey=LOCK_SHOP_KEY+id;
        boolean isLock=tryLock(lockKey);
        if (isLock)
        {
            //开启独立线程构建缓存(构建线程池实现)
            try {
                CACHE_REBUILD_EXECUTOR.submit(()->{
                    //重建
                    this.saveShop2Redis(id,20L);
                });
            } catch (Exception e)
            {
                throw new RuntimeException(e);
            }finally {
             unLock(lockKey);
            }
        }
        return shop;
    }

}
