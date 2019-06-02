package com.tomansill.old.redis.test.util;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/** Class to hold functions for tests
 *  @author <a href="mailto:tom@ansill.com">Tom Ansill</a>
 */
public class TestFunction {

    private TestFunction(){} // Prevents instantiation

	public static boolean performSimpleWriteLock(final ReadWriteLock rwl, final long max_time_out, final TimeUnit unit, final boolean debug){

		// Create executor service
		ExecutorService es = Executors.newCachedThreadPool();

		// Sensitive data
		final SensitiveData data = new SensitiveData();

		// CDL
		final ResetableCountDownLatch jobs = new ResetableCountDownLatch(2);
		final ResetableCountDownLatch rcdl = new ResetableCountDownLatch(1);

		// Set up future
		Future<Boolean> main_future = es.submit(() -> {

			try{

				// Get lock
				Lock lock = rwl != null ? rwl.writeLock() : null;

				// Lock it
				if(lock != null) lock.lock();

				// Tell other threads to go ahead and lock
				rcdl.countDown();

				// Manipulate
				data.set(1, TimeUnit.SECONDS, debug);

				// Unlock
				if(lock != null) lock.unlock();

				// Mark job as complete
				jobs.countDown();

				// Exit
				return true;

			}catch(Exception e){
				e.printStackTrace();
				return false;
			}
		});

		// Set up future
		Future<Boolean> write_future = es.submit(() -> {
			try{

				// Wait for main to lock
				rcdl.await();

				// Get lock
				Lock lock = rwl != null ? rwl.writeLock() : null;

				// Lock it
				if(lock != null) lock.lock();

				// Manipulate
				data.set(1, TimeUnit.SECONDS, debug);

				// Unlock
				if(lock != null) lock.unlock();

				// Mark job as complete
				jobs.countDown();

				// Exit
				return true;

			}catch(Exception e){
				e.printStackTrace();
				return false;
			}
		});

		/*
		// Set up future
		Future<Boolean> read_future = es.submit(() -> {
			try{

				// Wait for main to lock
				rcdl.await();

				// Get lock
				Lock lock = rwl != null ? rwl.readLock() : null;

				// Lock it
				if(lock != null) lock.lock();

				// Read
				data.read(1, TimeUnit.SECONDS);

				// Unlock
				if(lock != null) lock.unlock();

				// Exit
				return true;

			}catch(Exception e){
				e.printStackTrace();
				return false;
			}
		});
		*/

		// Run it and await for result
		try{
			if(!jobs.await(max_time_out, unit)){
				if(debug) System.err.println("Test Timed out");
				return false;
			}
			if(!main_future.isDone() || !main_future.get()) return false;
			if(!write_future.isDone() || !main_future.get()) return false;
			//if(!read_future.get(max_time_out, unit)) return false;
		}catch(InterruptedException | ExecutionException e){
			if(debug) System.err.println("Test Timed out");
			return false;
		}

    	return !data.isCorrupted();
	}

	public static boolean performSimpleReadLock(final ReadWriteLock rwl, final long max_time_out, final TimeUnit unit, final boolean debug){

		// Create executor service
		ExecutorService es = Executors.newCachedThreadPool();

		// Sensitive data
		final SensitiveData data = new SensitiveData();

		// CDL
		final ResetableCountDownLatch jobs = new ResetableCountDownLatch(3);
		final ResetableCountDownLatch rcdl = new ResetableCountDownLatch(1);

		// Set up future
		Future<Boolean> first_write_future = es.submit(() -> {
			try{

				// Get lock
				Lock lock = rwl != null ? rwl.writeLock() : null;

				// Lock it
				if(lock != null) lock.lock();

				// Tell other threads to go ahead and lock
				rcdl.countDown();

				// Manipulate
				data.set(1, TimeUnit.SECONDS, debug);

				// Unlock
				if(lock != null) lock.unlock();

				// Mark job as complete
				jobs.countDown();

				// Exit
				return true;

			}catch(Exception e){
				e.printStackTrace();
				return false;
			}
		});

		// Set up runnable
		Callable<Boolean> second_callable = () -> {
			try{

				// Wait for main to lock
				rcdl.await();

				// Get lock
				Lock lock = rwl != null ? rwl.readLock() : null;

				// Lock it
				if(lock != null) lock.lock();

				System.out.println("Allowed!");

				// Read
				data.read(1, TimeUnit.SECONDS);

				// Unlock
				if(lock != null) lock.unlock();

				// Mark job as complete
				jobs.countDown();

				// Exit
				return true;

			}catch(Exception e){
				e.printStackTrace();
				return false;
			}
		};

		// Set up future
		Future<Boolean> first_read_lock1 = es.submit(second_callable);
		Future<Boolean> first_read_lock2 = es.submit(second_callable);


		// Run it and await for result
		try{
			if(!jobs.await(max_time_out, unit)){
				if(debug) System.err.println("Test Timed out");
				return false;
			}
			if(!first_write_future.isDone() || !first_write_future.get()) return false;
			if(!first_read_lock1.isDone() || !first_read_lock1.get()) return false;
			if(!first_read_lock2.isDone() || !first_read_lock2.get()) return false;
		}catch(InterruptedException | ExecutionException e){
			if(debug) System.err.println("Test Timed out");
			return false;
		}

		// Check if data is still good
		if(data.isCorrupted()) return false;

		// Reset
		jobs.reset(4);
		rcdl.reset(1);
		ResetableCountDownLatch rcdl1 = new ResetableCountDownLatch(1);

		// Create read future
		Callable<Boolean> second_read_callable = () -> {
			try{

				// Get lock
				Lock lock = rwl != null ? rwl.readLock() : null;

				// Lock it
				if(lock != null) lock.lock();

				// Tell other threads to go ahead and lock
				rcdl.countDown();

				// Manipulate
				data.read(1, TimeUnit.SECONDS);

				// Unlock
				if(lock != null) lock.unlock();

				// Mark job as complete
				jobs.countDown();

				// Tell that third readlock to lock
				rcdl1.countDown();

				// Exit
				return true;

			}catch(Exception e){
				e.printStackTrace();
				return false;
			}
		};
		Future<Boolean> second_read_future1 = es.submit(second_read_callable);
		Future<Boolean> second_read_future2 = es.submit(second_read_callable);

		// Set up future
		Future<Boolean> second_write_future = es.submit(() -> {
			try{

				// Wait for main to lock
				rcdl.await();

				// Get lock
				Lock lock = rwl != null ? rwl.writeLock() : null;

				// Lock it
				if(lock != null) lock.lock();

				// Manipulate
				data.set(1, TimeUnit.SECONDS, debug);

				// Unlock
				if(lock != null) lock.unlock();

				// Mark job as complete
				jobs.countDown();

				// Exit
				return true;

			}catch(Exception e){
				e.printStackTrace();
				return false;
			}
		});

		// Set up future
		Future<Boolean> third_read_future = es.submit(() -> {
			try{

				// Wait for main to unlock
				rcdl1.await();

				// Get lock
				Lock lock = rwl != null ? rwl.readLock() : null;

				// Lock it
				if(lock != null) lock.lock();

				// Manipulate
				data.set(1, TimeUnit.SECONDS, debug);

				// Unlock
				if(lock != null) lock.unlock();

				// Mark job as complete
				jobs.countDown();

				// Exit
				return true;

			}catch(Exception e){
				e.printStackTrace();
				return false;
			}
		});

		// Run it and await for result
		try{
			if(!jobs.await(max_time_out, unit)){
				if(debug) System.err.println("Test Timed out");
				return false;
			}
			if(!second_read_future1.isDone() || !second_read_future1.get()) return false;
			if(!second_read_future2.isDone() || !second_read_future2.get()) return false;
			if(!second_write_future.isDone() || !second_write_future.get()) return false;
			if(!third_read_future.isDone() || !third_read_future.get()) return false;

		}catch(InterruptedException | ExecutionException e){
			if(debug) System.err.println("Test Timed out");
			return false;
		}

		return !data.isCorrupted();
	}

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
