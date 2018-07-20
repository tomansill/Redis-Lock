package com.tomansill.redis.jedis;

import com.tomansill.redis.lock.TestMultiInstance;
import com.tomansill.redis.lock.TestSingleInstance;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class TestJedisLockClient {

    private final static int INSTANCE_NUMBER = 5;

    private static JedisPool[] pools = null;
    private static JedisLockClient[] clients = null;

    @BeforeClass
    public static void setUp(){

        // Initialize instance arrays
        pools = new JedisPool[INSTANCE_NUMBER];
        clients = new JedisLockClient[INSTANCE_NUMBER];

        // Initialize instances
        for(int i = 0; i < INSTANCE_NUMBER; i++){
            pools[i] = new JedisPool(new JedisPoolConfig(), TestSingleInstance.HOSTNAME, TestSingleInstance.PORT);
            clients[i] = new JedisLockClient(pools[i]);
        }

        TestSingleInstance.setUp(clients[0]);
        TestMultiInstance.setUp(clients);
    }

    @AfterClass
    public static void tearDown(){
        for(JedisPool pool : pools) pool.close();
    }


    @Test
    public void testMultipleWriteLockOnSingleInstance(){
        TestSingleInstance.testMultipleWriteLocks();
    }

    @Test
    public void testMultipleWriteLockOnMultiInstance(){
        TestMultiInstance.testMultipleWriteLocks();
    }
}
