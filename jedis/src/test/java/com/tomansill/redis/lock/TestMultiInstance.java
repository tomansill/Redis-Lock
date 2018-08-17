package com.tomansill.redis.lock;

import com.tomansill.redis.test.util.TestFunction;

import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

public class TestMultiInstance{

    private static AbstractRedisLockClient[] clients = null;
    public final static String HOSTNAME = "localhost";
    public final static int PORT = 6379;

    private TestMultiInstance(){}

    public static void setUp(AbstractRedisLockClient[] in_clients){

        assertNotNull("in_clients is null!", in_clients);

        for(int i = 0; i < in_clients.length; i++){
            assertNotNull("Client #" + i + " is null!", in_clients[i]);
        }

        clients = in_clients;
    }

    @SuppressWarnings("unchecked")
    public static void testMultipleWriteLocks(final boolean debug){

        // Check database connection
        assumeTrue("We are not connected to Redis server, this test cannot continue.",clients != null);

        // Set num threads
        int num_threads = 10;

        // Create RRWL for control test
        final ReentrantReadWriteLock rrwl = new ReentrantReadWriteLock();

        // Create executor service
        ExecutorService es = Executors.newCachedThreadPool();

        // Create threads
        Future<Boolean>[] futures = new Future[clients.length];

        // Finalize clients
        final AbstractRedisLockClient[] f_clients = clients;

        // Iterate threads to build threads for control test number one
        for(int i = 0; i < f_clients.length; i++){
            futures[i] = es.submit(() -> com.tomansill.redis.test.util.TestFunction.performMultipleWriteLock(null, num_threads, debug));
        }

        // Do control test 1
        for(Future<Boolean> future : futures){
            try {
                assertTrue("The control test number one has failed, the test is flawed.", !future.get());
            }catch(InterruptedException | ExecutionException e){
                fail("InterruptedException was thrown. Reason: " + e.getMessage());
            }
        }

        // Iterate threads to build threads for control test number one
        for(int i = 0; i < f_clients.length; i++){
            futures[i] = es.submit(() -> com.tomansill.redis.test.util.TestFunction.performMultipleWriteLock(rrwl, num_threads, debug));
        }

        // Do control test 2
        for(Future<Boolean> future : futures){
            try {
                assertTrue("The control test number two has failed, the test is flawed.", future.get());
            }catch(InterruptedException | ExecutionException e){
                fail("InterruptedException was thrown. Reason: " + e.getMessage());
            }
        }

        // Iterate threads to build threads for experiment unfair test
        final String lockpoint1 = Utility.generateRandomString(8);
        for(int i = 0; i < f_clients.length; i++){
            final int index = i;
            futures[i] = es.submit(() -> com.tomansill.redis.test.util.TestFunction.performMultipleWriteLock(f_clients[index].getLock(lockpoint1), num_threads, 5, TimeUnit.SECONDS, debug));
        }

        // Do experiment test with unfair locking
        for(Future<Boolean> future : futures){
            try {
                assertTrue("The unfair experiment test has failed.", future.get());
            }catch(InterruptedException | ExecutionException e){
                fail("InterruptedException was thrown. Reason: " + e.getMessage());
            }
        }

        // Iterate threads to build threads for experiment fair test
        final String lockpoint2 = Utility.generateRandomString(8);
        for(int i = 0; i < f_clients.length; i++){
            final int index = i;
            futures[i] = es.submit(() -> TestFunction.performMultipleWriteLock(f_clients[index].getLock(lockpoint2, true), num_threads, 5, TimeUnit.SECONDS, debug));
        }

        // Do experiment test with fair locking
        for(Future<Boolean> future : futures){
            try {
                assertTrue("The fair experiment test has failed.", future.get());
            }catch(InterruptedException | ExecutionException e){
                fail("InterruptedException was thrown. Reason: " + e.getMessage());
            }
        }
    }
}
