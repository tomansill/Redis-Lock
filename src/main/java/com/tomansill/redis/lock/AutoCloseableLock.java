package com.tomansill.redis.lock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public interface AutoCloseableLock extends AutoCloseable, Lock{

    /** Performs lock() and returns the lock
     *  @return Locked lock
     */
    public Lock doLock();

    /** Performs lockInterruptibly() and returns the lock
     *  @return Locked lock
     *  @throws InterruptedException if the current thread is interrupted while acquiring the lock (and interruption of lock acquisition is supported)
     */
    public Lock doLockInterruptibly() throws InterruptedException;
}
