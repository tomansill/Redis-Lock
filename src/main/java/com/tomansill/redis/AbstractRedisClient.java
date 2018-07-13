package com.tomansill.redis;

import com.tomansill.redis.lock.RedisReadWriteLock;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.Map;

/** AbstractRedisClient class
 *  This class is abstract and defines methods for subclasses to implement with their own Redis client.
 *  @author <a href="mailto:tom@ansill.com">Tom Ansill</a>
 */
public abstract class AbstractRedisClient{

    // ##### PUBLIC STATIC MEMBERS #####

    /** Default lease duration */
    public final static long DEFAULT_LEASE_DURATION_SECONDS = 60;

    // ##### PRIVATE STATIC MEMBERS #####

    /* Map of lease durations per server */
    private final static Map<String,Duration> LEASE_DURATION = new HashMap<String,Duration>(1);

    /* Lock for lease duration map */
    private final static ReentrantReadWriteLock LEASE_LOCK = new ReentrantReadWriteLock(true);

    /* Comfy little class that holds time and time unit */
    private static class Duration{
        private long time;
        private TimeUnit unit;
        public Duration(final long time, final TimeUnit unit){
            this.time = time;
            this.unit = unit;
        }
        public synchronized long getTime(){
            return this.time;
        }
        public synchronized TimeUnit getUnit(){
            return this.unit;
        }
        public synchronized void set(final long time, final TimeUnit unit){
            this.time = time;
            this.unit = unit;
        }
    }

    // ##### CLASS MEMBERS #####

    /** Returns host name and port in format of (hostname):(port)
     *  @return string format of (hostname):(port)
     */
    protected abstract String getHostnameAndPort();

    /** Returns true if this client is connected to a cluster, false otherwise
     *  @return true if this client is connected to a cluster, false otherwise
     */
    public abstract boolean isCluster();

    /** Retrieves current lease duration
     *  @param unit TimeUnit for returned time
     *  @return time in time unit provided in the parameter
     *  @throws IllegalArgumentException thrown if unit parameter is null
     */
    public long getLeaseDuration(final TimeUnit unit) throws IllegalArgumentException{

        // Check unit
        if(unit == null) throw new IllegalArgumentException("unit is null");

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

    /** Sets lease duration
     *  @param time time
     *  @param unit TimeUnit
     *  @throws IllegalArgumentException thrown if unit parameter is null or time is a negative number
     */
    public void setLeaseDuration(final long time, final TimeUnit unit){

        // Check unit and time
        if(unit == null) throw new IllegalArgumentException("unit is null");
        if(time < 0) throw new IllegalArgumentException("time is not positive");

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

    public RedisReadWriteLock getLock(final String lockpoint){
        return new RedisReadWriteLock(lockpoint, this);
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

    protected abstract String get();

}
