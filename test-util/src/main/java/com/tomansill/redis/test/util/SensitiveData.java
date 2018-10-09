package com.tomansill.redis.test.util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** Sensitive data object, used for testing concurrency mechanisms
 *  @author <a href="mailto:tom@ansill.com">Tom Ansill</a>
 */
public class SensitiveData {

    /** Flag to indicate that the data is corrupted */
    private boolean corrupted = false;

    /** Control unit */
    private AtomicBoolean write = new AtomicBoolean(false);

    /** Internal data */
    private AtomicLong counter = new AtomicLong(0);

    /** Constructor for Sensitive Data object */
    public SensitiveData(){}

    /** Resets the data, clearing away the corruption */
    public void reset(){
        this.corrupted = false;
    }

    /** Attempts to modify the data
     *  @param time time to wait
     *  @param unit unit of time
     */
    public void set(final long time, TimeUnit unit, final boolean quiet){
        this.set(time, unit, null, quiet);
    }

    /** Attempts to modify the data
     *  @param time time to wait
     *  @param unit unit of time
     *  @param cdl countDown at the beginning of the data modification, will not countDown if null
     */
    public void set(final long time, TimeUnit unit, final CountDownLatch cdl, final boolean quiet){
        if(this.write.compareAndSet(false, true)){
            if(cdl != null) cdl.countDown();
            try{
                Thread.sleep(TimeUnit.MILLISECONDS.convert(time, unit));
            }catch(InterruptedException e){e.printStackTrace();}
	        counter.getAndIncrement();
	        this.write.set(false);
        }else{
            if(!quiet){
                System.out.println("Corrupted!");
                StackTraceElement[] ste = Thread.currentThread().getStackTrace();
                for(StackTraceElement element : ste){
                    System.out.println(element);
                }
            }
            this.corrupted = true;
        }
    }

    public void read(final long time, TimeUnit unit){
    	long result = counter.get();
	    try{
		    Thread.sleep(TimeUnit.MILLISECONDS.convert(time, unit));
	    }catch(InterruptedException e){e.printStackTrace();}
	    if(result != counter.get()) this.corrupted = true;
    }

    /** Returns true if corrupted, otherwise false
     *  @return true if corrupted, otherwise false
     */
    public boolean isCorrupted(){
        return this.corrupted;
    }
}
