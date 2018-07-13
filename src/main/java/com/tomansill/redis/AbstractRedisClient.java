package com.tomansill.redis;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.Map;

public abstract class AbstractRedisClient{

    // ##### PUBLIC STATIC MEMBERS #####
    /** Default lease duration */
    public static final long DEFAULT_LEASE_DURATION_SECONDS = 60;

    // ##### PRIVATE STATIC MEMBERS #####
    /* Map of lease durations per server */
    private static Map<String,Duration> LEASE_DURATION = new HashMap<String,Duration>(1);

    /* Lock for lease duration map */
    private static ReentrantReadWriteLock LEASE_LOCK = new ReentrantReadWriteLock(true);

    /* Comfy little class that holds time and time unit */
    private static class Duration{
        private long time;
        private TimeUnit unit;
        public Duration(long time, TimeUnit unit){
            this.time = time;
            this.unit = unit;
        }
        public synchronized long getTime(){
            return this.time;
        }
        public synchronized TimeUnit getUnit(){
            return this.unit;
        }
        public synchronized void set(long time, TimeUnit unit){
            this.time = time;
            this.unit = unit;
        }
    }

    protected abstract String getHostnameAndPort();

    protected abstract String get() throws IOException;

    protected abstract boolean isCluster() throws IOException;

    public long getLeaseDuration(TimeUnit unit){

        // Result container
        long result = 0;

        // Get URL
        String url = this.getHostnameAndPort();

        // Obtain read lock and lock it
        Lock read_lock = LEASE_LOCK.readLock();
        read_lock.lock();

        // Retrieve time if it exists, otherwise use default one
        if(LEASE_DURATION.containsKey(url)){
            Duration duration = LEASE_DURATION.get(url);
            result = unit.convert(duration.getTime(), duration.getUnit());
        }else{
            result = unit.convert(DEFAULT_LEASE_DURATION_SECONDS, TimeUnit.SECONDS);
        }

        // Release lock
        read_lock.unlock();

        // Return it
        return result;
    }

    public void setLeaseDuration(long time, TimeUnit unit){

        // Get URL
        String url = this.getHostnameAndPort();

        // Obtain write lock and lock it
        Lock write_lock = LEASE_LOCK.writeLock();
        write_lock.lock();

        // Insert or update
        if(LEASE_DURATION.containsKey(url)){
            LEASE_DURATION.get(url).set(time, unit);
        }else{
            LEASE_DURATION.put(url, new Duration(time, unit));
        }

        // Release lock
        write_lock.unlock();
    }

    public boolean isFairSupported(){
        return false; // For now TODO
    }

    public boolean isUnfairSupported(){
        return true;
    }

    public boolean isExclusiveLockSupported(){
        return true;
    }

    public boolean isInclusiveLockSupported(){
        return false; // For now TODO
    }

    public boolean isNewConditionSupported(){
        return false; // For now TODO
    }

    public boolean isExclusiveNewConditionSupported(){
        return false; // For now TODO
    }

    public boolean isInclusiveNewConditionSupported(){
        return false; // For now TODO
    }

    public boolean isExclusiveEvictionSupported(){
        return true;
    }

    public boolean isInclusiveEvictionSupported(){
        return true;
    }

    public boolean isNonExclusiveEvictionSupported(){
        return true;
    }

    public boolean isNonInclusiveEvictionSupported(){
        return true;
    }
}
