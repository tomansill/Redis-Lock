package com.tomansill.redis.lock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public interface AutoCloseableRedisLock extends AutoCloseableLock, RedisLock{

    /** Performs lock() and returns the lock
     *  @param lease_time lock lease time
     *  @param unit the time unit of the time argument
     *  @return Locked lock
     */
    public Lock doLock(TimeUnit unit, long lease_time);

    /** Performs lockInterruptibly() and returns the lock
     *  @param lease_time lock lease time
     *  @param unit the time unit of the time argument
     *  @return Locked lock
     *  @throws InterruptedException if the current thread is interrupted while acquiring the lock (and interruption of lock acquisition is supported)
     */
    public Lock doLockInterruptibly(TimeUnit unit, long lease_time) throws InterruptedException;
}
