package com.tomansill.redis.lock;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

    static class SensitiveData{
        private boolean corrupted = false;
        private AtomicBoolean write = new AtomicBoolean(false);
        public SensitiveData(){}

        public void reset(){
            this.corrupted = false;
        }

        public void set(final long time, TimeUnit unit, final String id){
            System.out.println("#####start: " + id);
            if(this.write.compareAndSet(false, true)){
                System.out.println("#####locked: " + id);
                try{
                    Thread.sleep(TimeUnit.MILLISECONDS.convert(time, unit));
                }catch(InterruptedException e){e.printStackTrace();}
                this.write.set(false);
                System.out.println("#####unlocked: " + id);
            }else{
                System.out.println("#####corrupted: " + id);
                this.corrupted = true;
            }
        }

        public boolean isCorrupted(){
            return this.corrupted;
        }
    }

    public static void testSingleWriteLock() {

        // Check database connection
        assumeTrue("We are not connected to Redis server, this test cannot continue.", client != null);

        // Lock it!
        try(AutoCloseableRedisLock lock = client.getLock("test_lockpoint").writeLock().doLock()){
            System.out.println("locked!");
        }catch(Exception e){
            assertTrue("Lock failed! Exception message: " + e.getMessage(), false);
        }
    }

    public static void testWriteLock(){

        // Check database connection
        assumeTrue("We are not connected to Redis server, this test cannot continue.",client != null);

        Thread[] threads = new Thread[2];

        final SensitiveData data = new SensitiveData();
        final CountDownLatch test1 = new CountDownLatch(1);

        final ReentrantReadWriteLock rrwl = new ReentrantReadWriteLock();

        threads[0] = new Thread(() -> {
            // Lock it!
            Lock lock = rrwl.writeLock();
            lock.lock();
            test1.countDown();
            data.set(5, TimeUnit.SECONDS, "1");
            lock.unlock();
        });

        threads[1] = new Thread(() -> {
            // Wait for thread 1
            try {
                test1.await();
            }catch(Exception e){
                assertTrue("Lock failed! Exception message: " + e.getMessage(), false);
            }

            // Lock it!
            Lock lock = rrwl.writeLock();
            lock.lock();
            data.set(100, TimeUnit.MILLISECONDS, "2");
            lock.unlock();

        });

        // Start all
        for(Thread thread : threads) thread.start();

        // Join all
        try {
            for (Thread thread : threads) thread.join();
        }catch(InterruptedException e){
            assertTrue("Exception thrown! " + e.getMessage(), false);
        }

        assertTrue("Control test failed, the test is flawed", !data.isCorrupted());

        System.out.println("###### NEW TEST #######");

        final CountDownLatch test2 = new CountDownLatch(1);
        data.reset();

        threads[0] = new Thread(() -> {
            // Lock it!
            try(AutoCloseableRedisLock lock = client.getLock("test_lockpoint").writeLock().doLock()){
                test2.countDown();
                data.set(5, TimeUnit.SECONDS, "1");
                System.out.println("done!");
            }catch(Exception e){
                assertTrue("Lock failed! Exception message: " + e.getMessage(), false);
            }
        });

        threads[1] = new Thread(() -> {
            // Wait for thread 1
            try {
                test2.await();
                System.out.println("done waiting!");
            }catch(Exception e){
                assertTrue("Lock failed! Exception message: " + e.getMessage(), false);
            }

            // Lock it!
            try(AutoCloseableRedisLock lock = client.getLock("test_lockpoint").writeLock().doLock()){
                System.out.println("locked! id:2");
                data.set(100, TimeUnit.MILLISECONDS, "2");
            }catch(Exception e){
                assertTrue("Lock failed! Exception message: " + e.getMessage(), false);
            }
        });

        // Start all
        for(Thread thread : threads) thread.start();

        // Join all
        try {
            for (Thread thread : threads) thread.join();
        }catch(InterruptedException e){
            assertTrue("Exception thrown! " + e.getMessage(), false);
        }

        // Check data
        assertTrue("Locking did not succeed. Object is corrupted", !data.isCorrupted());
    }
}