package com.tomansill.redis.jedis;

import com.tomansill.redis.lock.TestAbstractRedisLockClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class TestJedisLockClient {

    private static JedisPool pool = null;
    private static JedisLockClient client = null;

    @BeforeClass
    public static void setUp(){
        pool = new JedisPool(new JedisPoolConfig(), TestAbstractRedisLockClient.HOSTNAME + ":" + TestAbstractRedisLockClient.PORT);
        client = new JedisLockClient(pool);
        TestAbstractRedisLockClient.setUp(client);
    }

    @AfterClass
    public static void tearDown(){
        pool.close();
    }

    @Test
    public void testWriteLock(){
        TestAbstractRedisLockClient.testWriteLock();
    }
}
