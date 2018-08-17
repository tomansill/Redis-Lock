package com.tomansill.redis.lock;

import com.tomansill.redis.test.util.TestFunction;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class TestSingleInstance {

    private static AbstractRedisLockClient client = null;
    public final static String HOSTNAME = "localhost";
    public final static int PORT = 6379;

    private TestSingleInstance(){}

    public static void setUp(AbstractRedisLockClient in_client){

        assertNotNull("in_client is null!", in_client);

        client = in_client;
    }

    public static void testMultipleWriteLocks(final boolean debug){

        // Check database connection
        assumeTrue("We are not connected to Redis server, this test cannot continue.",client != null);

        // Set num threads
        int num_threads = 20;

        // Do control test
        assertTrue("The control test number one has failed, the test is flawed.", !com.tomansill.redis.test.util.TestFunction.performMultipleWriteLock(null, num_threads, debug));

        // Do control test
        assertTrue("The control test number two has failed, the test is flawed.", com.tomansill.redis.test.util.TestFunction.performMultipleWriteLock(new ReentrantReadWriteLock(), num_threads, debug));

        // Do experiment test with unfair locking
        assertTrue("The unfair experiment test has failed.", com.tomansill.redis.test.util.TestFunction.performMultipleWriteLock(client.getLock(Utility.generateRandomString(8), false), num_threads, 5, TimeUnit.SECONDS, debug));

        // Do experiment test with fair locking
        assertTrue("The fair experiment test has failed.", com.tomansill.redis.test.util.TestFunction.performMultipleWriteLock(client.getLock(Utility.generateRandomString(8), true), num_threads, 5, TimeUnit.SECONDS, debug));

    }

    public static void testMultipleWriteLocksFairLock(final boolean debug){

        // Check database connection
        assumeTrue("We are not connected to Redis server, this test cannot continue.",client != null);

        // Set num threads
        int num_threads = 20;

        // Do control test
        assertTrue("The control test number two has failed, the test is flawed.", com.tomansill.redis.test.util.TestFunction.performMultipleFairLock(new ReentrantReadWriteLock(true), num_threads, debug));

        // Do experiment test with fair locking
        assertTrue("The fair experiment test has failed.", TestFunction.performMultipleFairLock(client.getLock(Utility.generateRandomString(8), true), num_threads, 5, TimeUnit.SECONDS, debug));

    }
}