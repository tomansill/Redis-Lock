package com.tomansill.redis.lock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class TestSingleInstance {

    private static AbstractRedisLockClient client = null;
    public final static String HOSTNAME = "localhost";
    public final static int PORT = 6379;

    private TestSingleInstance(){}

    public static void setUp(AbstractRedisLockClient in_client){

        assertTrue("in_client is null!", in_client != null);

        client = in_client;
    }

    public static void testMultipleWriteLocks(){

        // Check database connection
        assumeTrue("We are not connected to Redis server, this test cannot continue.",client != null);

        // Set num threads
        int num_threads = 20;

        // Do control test
        assertTrue("The control test number one has failed, the test is flawed.", !TestFunction.performMultipleWriteLock(null, num_threads));

        // Do control test
        assertTrue("The control test number two has failed, the test is flawed.", TestFunction.performMultipleWriteLock(new ReentrantReadWriteLock(), num_threads));

        // Do experiment test with unfair locking
        assertTrue("The unfair experiment test has failed.", TestFunction.performMultipleWriteLock(client.getLock(Utility.generateRandomString(8), false), num_threads, 5, TimeUnit.SECONDS));

        // Do experiment test with fair locking
        assertTrue("The fair experiment test has failed.", TestFunction.performMultipleWriteLock(client.getLock(Utility.generateRandomString(8), true), num_threads, 5, TimeUnit.SECONDS));

    }
}