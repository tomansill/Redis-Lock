package com.tomansill.redis.jedis;

import com.tomansill.redis.lock.TestMultiInstance;
import com.tomansill.redis.lock.TestSingleInstance;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class TestJedisLockClient {

    private final static int INSTANCE_NUMBER = 3;

    private static JedisPool[] pools = null;

	private static final boolean DEBUG = true;

    @BeforeClass
    public static void setUp() throws Exception{

        // Initialize instance arrays
        pools = new JedisPool[INSTANCE_NUMBER];
	    JedisLockClient[] clients = new JedisLockClient[INSTANCE_NUMBER];

        // Initialize instances
        for(int i = 0; i < INSTANCE_NUMBER; i++){
            pools[i] = new JedisPool(new JedisPoolConfig(), TestSingleInstance.HOSTNAME, TestSingleInstance.PORT);
            pools[i].getResource().ping();
            clients[i] = new JedisLockClient(pools[i]);
        }

        TestSingleInstance.setUp(clients[0]);
        TestMultiInstance.setUp(clients);

        // For profiler
        //System.out.println("Waiting 10 seconds...");
	    //Thread.sleep(10000);
    }

    @AfterClass
    public static void tearDown(){
        for(JedisPool pool : pools) pool.close();
    }


	@Test
	public void testUnfairWriteLock(){
		if(DEBUG) System.out.println("Performing unfair write lock on single instance test...");
		TestSingleInstance.testWriteLock(false, DEBUG);
		if(DEBUG) System.out.println("Unfair write locks test on single instance complete!");
	}

	@Test
	public void testFairWriteLock(){
		if(DEBUG) System.out.println("Performing fair write lock on single instance test...");
		TestSingleInstance.testWriteLock(true, DEBUG);
		if(DEBUG) System.out.println("Fair write locks test on single instance complete!");
	}

	@Test
	public void testUnfairReadLock(){
		if(DEBUG) System.out.println("Performing unfair read lock on single instance test...");
		TestSingleInstance.testReadLock(false, DEBUG);
		if(DEBUG) System.out.println("Unfair read locks test on single instance complete!");
	}

	@Test
	public void testFairReadLock(){
		if(DEBUG) System.out.println("Performing fair read lock on single instance test...");
		TestSingleInstance.testReadLock(true, DEBUG);
		if(DEBUG) System.out.println("Fair read locks test on single instance complete!");
	}

}
