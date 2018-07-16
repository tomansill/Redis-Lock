package com.tomansill.redis.lock;

import java.util.concurrent.locks.Lock;

public interface AutoCloseableLock extends AutoCloseable, Lock{

    /** Performs lock() and returns the lock
     *  @return Locked lock
     */
    Lock doLock();

    /** Performs lockInterruptibly() and returns the lock
     *  @return Locked lock
     *  @throws InterruptedException if the current thread is interrupted while acquiring the lock (and interruption of lock acquisition is supported)
     */
    Lock doLockInterruptibly() throws InterruptedException;
}
