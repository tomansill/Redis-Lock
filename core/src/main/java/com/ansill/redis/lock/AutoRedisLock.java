package com.ansill.redis.lock;

import com.ansill.lock.autolock.AutoLock;
import com.ansill.lock.autolock.LockedAutoLock;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface AutoRedisLock extends AutoLock{

    /** Performs lock() and returns the lock
     *  @param lease_time lock lease time
     *  @param unit the time unit of the time argument
     *  @return Locked lock
     */
    @Nonnull
    LockedAutoLock doLock(@Nonnull TimeUnit unit, @Nonnegative long lease_time);

    /** Performs lockInterruptibly() and returns the lock
     *  @param lease_time lock lease time
     *  @param unit the time unit of the time argument
     *  @return Locked lock
     *  @throws InterruptedException if the current thread is interrupted while acquiring the lock (and interruption of lock acquisition is supported)
     */
    @Nonnull
    LockedAutoLock doLockInterruptibly(@Nonnull TimeUnit unit, @Nonnegative long lease_time)
    throws InterruptedException;

    @Nonnull
    LockedAutoLock doTryLock(@Nonnull TimeUnit unit, @Nonnegative long lease_time) throws TimeoutException;

    @Nonnull
    LockedAutoLock doTryLock(@Nonnegative long wait_time, @Nonnull TimeUnit unit, @Nonnegative long lease_time)
    throws TimeoutException, InterruptedException;
}
