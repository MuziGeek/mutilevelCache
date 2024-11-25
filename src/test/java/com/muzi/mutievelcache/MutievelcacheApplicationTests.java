package com.muzi.mutievelcache;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.concurrent.ConcurrentMapCache;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;

@SpringBootTest
class MutievelcacheApplicationTests {

    @Test
    void contextLoads() {
    }

    @Test
    public void test() {
        ConcurrentMapCache cache = new ConcurrentMapCache("test");
        cache.put("1", "a");
        cache.get("2", () -> null);
        System.out.println(cache.get("1").get());
        System.out.println(cache.get("2").get());

        cache.clear();
        System.out.println(cache.get("1"));
        System.out.println(cache.get("2"));
    }



        @Test
        public void test1() {
            ConcurrentMapCacheManager cacheManager
                    = new ConcurrentMapCacheManager("test1", "test2");
            cacheManager.getCache("test1")
                    .get("1", () -> "a");

            // 指定了 cacheNames，无法创建缺省缓存，NPE
            cacheManager.getCache("test3")
                    .get("1", () -> "b");


    }


}
