package com.tomansill.redis.lock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/** Class to hold functions for tests
 *  @author <a href="mailto:tom@ansill.com">Tom Ansill</a>
 */
public class TestFunction {

    private TestFunction(){} // Prevents instantiation

    public static boolean performMultipleWriteLock(final ReadWriteLock rwl, final int num_threads) {
        return performMultipleWriteLock(rwl, num_threads, 0, null);
    }

    public static boolean performMultipleWriteLock(final ReadWriteLock rwl, final int num_threads, final int max_time_out, final TimeUnit unit){

        // Initialize threads
        final Thread[] threads = new Thread[num_threads];

        // Initialize test object
        final SensitiveData data = new SensitiveData();

        // Initialize control
        final AtomicBoolean cont = new AtomicBoolean(true);

        // Create threads
        for(int i = 0; i < threads.length; i++) threads[i] = new Thread(() -> {

            // Try and catch block
            try {

                // If cont is set to false, then test has failed and should exit
                if (cont.get()) {

                    // Obtain a lock
                    Lock lock = null;
                    if (rwl != null) lock = rwl.writeLock();

                    // Lock it
                    if (lock != null && unit == null) lock.lock();
                    if (lock != null && unit != null){
                        if(!lock.tryLock(max_time_out, unit)){
                            System.out.println("Timed out");
                            cont.set(false);
                            return;
                        }
                    }

                    // Modify data
                    data.set(100, TimeUnit.MILLISECONDS, (lock == null));

                    // Unlock
                    if (lock != null) lock.unlock();
                }
            }catch(InterruptedException e){
                cont.set(false);
                e.printStackTrace();
            }
        });

        //Launch it
        for(Thread thread : threads) thread.start();

        // Join
        for(Thread thread : threads){
            try {
                thread.join();
            }catch(InterruptedException e){
                e.printStackTrace();
                return false;
            }
        }

        // Return result
        return !data.isCorrupted() && cont.get();
    }
}
