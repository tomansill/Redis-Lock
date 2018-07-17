package com.tomansill.redis.lock;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class TestAbstractRedisLockClient {

    private static AbstractRedisLockClient client = null;
    public final static String HOSTNAME = "localhost";
    public final static int PORT = 6379;

    public static void setUp(AbstractRedisLockClient in_client){

        assertTrue("Client is null!", in_client != null);

        client = in_client;
    }

    public static void testWriteLock(){

        // Check database connection
        assumeTrue("We are not connected to Redis server, this test cannot continue.",client != null);

        assertTrue(true);
    }
}