package com.tomansill.redis.test.util;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/** Class to hold functions for tests
 *  @author <a href="mailto:tom@ansill.com">Tom Ansill</a>
 */
public class TestFunction {

    private TestFunction(){} // Prevents instantiation

    public static boolean performMultipleFairLock(final ReadWriteLock rwl, final int num_threads, final boolean debug) {
        return performMultipleFairLock(rwl, num_threads, 0, null, debug);
    }

    public static boolean performMultipleFairLock(final ReadWriteLock rwl, final int num_threads, final int max_time_out, final  TimeUnit unit, final boolean debug) {

        // Initialize threads
        final Future[] futures = new Future[num_threads];

        // Initialize test object
        final AtomicInteger counter = new AtomicInteger(0);

        // Initialize control
        final AtomicBoolean cont = new AtomicBoolean(true);

        // Create executor service
        ExecutorService es = Executors.newCachedThreadPool();

        // Create futures
        for (int i = 0; i < futures.length; i++) {

            // Create future
            futures[i] = es.submit(() -> {

                // Result holder
                int result = -1;
                long elapsed = -1;

                // Lock
                Lock lock = null;

                // Try and catch block
                try {

                    // If cont is set to false, then test has failed and should exit
                    if (cont.get()) {

                        // Obtain a lock
                        if (rwl != null) lock = rwl.writeLock();

                        // Time it
                        elapsed = System.currentTimeMillis();

                        // Lock it
                        if (lock != null && unit == null) lock.lock();
                        if (lock != null && unit != null){
                            if(!lock.tryLock(max_time_out, unit)){
                                if(debug) System.err.println("Timed out");
                                cont.set(false);
                                return new TimeResult(-1, -1);
                            }
                        }

                        // Get elapsed time
                        elapsed = System.currentTimeMillis() - elapsed;

                        // Modify data
                        result = counter.incrementAndGet();


                        Thread.sleep(1000);
                    }
                }catch(InterruptedException e){
                    cont.set(false);
                    if(debug) e.printStackTrace();
                }finally {
                    // Unlock
                    if (lock != null) lock.unlock();
                }

                return new TimeResult(elapsed, result);
            });
        }

        try {

            // Get result
            for (Future future : futures) {
                TimeResult tr = (TimeResult) future.get();
                if (debug) System.out.println(tr.ranking + " " + tr.elapsed_time);
            }

        }catch(ExecutionException | InterruptedException e){
            if(debug) e.printStackTrace();
            return false;
        }

        return true;
    }

    public static boolean performMultipleWriteLock(final ReadWriteLock rwl, final int num_threads, final boolean debug) {
        return performMultipleWriteLock(rwl, num_threads, 0, null, debug);
    }

    public static boolean performMultipleWriteLock(final ReadWriteLock rwl, final int num_threads, final int max_time_out, final TimeUnit unit, final boolean debug){

        // Initialize threads
        final Thread[] threads = new Thread[num_threads];

        // Initialize test object
        final SensitiveData data = new SensitiveData();

        // Initialize control
        final AtomicBoolean cont = new AtomicBoolean(true);

        // Create threads
        for(int i = 0; i < threads.length; i++) threads[i] = new Thread(() -> {

            // Lock
            Lock lock = null;

            // Try and catch block
            try {

                // If cont is set to false, then test has failed and should exit
                if (cont.get()) {

                    // Obtain a lock
                    if (rwl != null) lock = rwl.writeLock();

                    // Lock it
                    if (lock != null && unit == null) lock.lock();
                    if (lock != null && unit != null){
                        if(!lock.tryLock(max_time_out, unit)){
                            if(debug) System.out.println("Timed out");
                            cont.set(false);
                            return;
                        }
                    }

                    // Modify data
                    data.set(100, TimeUnit.MILLISECONDS, (lock == null));

                }
            }catch(InterruptedException e){
                cont.set(false);
                e.printStackTrace();
            }finally {

                // Unlock
                if (lock != null) lock.unlock();
            }
        });

        //Launch it
        for(Thread thread : threads) thread.start();

        // Join
        for(Thread thread : threads){
            try {
                thread.join();
            }catch(InterruptedException e){
                if(debug) e.printStackTrace();
                return false;
            }
        }

        // Return result
        return !data.isCorrupted() && cont.get();
    }

    private static class TimeResult{
        long elapsed_time;
        int ranking;
        TimeResult(long elapsed_time, int ranking){
            this.elapsed_time = elapsed_time;
            this.ranking = ranking;
        }
    }
}
