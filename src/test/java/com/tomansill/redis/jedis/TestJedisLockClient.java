package com.tomansill.redis.jedis;

import com.tomansill.redis.lock.TestMultiInstance;
import com.tomansill.redis.lock.TestSingleInstance;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class TestJedisLockClient {

    private final static int INSTANCE_NUMBER = 3;

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
        System.out.println("Performing multiple write locks on single instance test...");
        TestSingleInstance.testMultipleWriteLocks();
        System.out.println("Single write locks test on single instance complete!");
    }

    @Test
    public void testMultipleWriteLockOnMultiInstance(){
        System.out.println("Performing multiple write locks on multiple instances test...");
        TestMultiInstance.testMultipleWriteLocks();
        System.out.println("Multiple write locks test on multiple instances complete!");
    }

    @Test
    @Ignore("Test doesn't work properly")
    public void testMultipleWriteLocksFairLockOnSingleInstance(){
        System.out.println("Performing multiple write locks ordering on single instance test...");
        TestSingleInstance.testMultipleWriteLocksFairLock();
        System.out.println("Single write locks test ordering on single instance complete!");
    }
}
