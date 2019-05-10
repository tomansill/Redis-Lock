package com.tomansill.redis.lock;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;

public interface AutoCloseableRedisLock extends AutoCloseableLock, RedisLock{

    /** Performs lock() and returns the lock
     *  @param lease_time lock lease time
     *  @param unit the time unit of the time argument
     *  @return Locked lock
     */
    AutoCloseableRedisLock doLock(@Nonnull TimeUnit unit, @Nonnegative long lease_time);

    /** Performs lockInterruptibly() and returns the lock
     *  @param lease_time lock lease time
     *  @param unit the time unit of the time argument
     *  @return Locked lock
     *  @throws InterruptedException if the current thread is interrupted while acquiring the lock (and interruption of lock acquisition is supported)
     */
    AutoCloseableRedisLock doLockInterruptibly(@Nonnull TimeUnit unit, @Nonnegative long lease_time) throws InterruptedException;

    @Override
    default void lock(@Nonnull TimeUnit unit, @Nonnegative long lease_time) {

    }
}
