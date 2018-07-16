package com.tomansill.redis.lock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/** Generic Lock class that provides some implementation to ReadLock and WriteLock subclasses */
abstract class GenericLock implements AutoCloseableRedisLock{

    /* ID Counter */
    private final static AtomicLong ID_COUNTER = new AtomicLong(Long.MIN_VALUE);

    /** Parent RedisReadWriteLock instance */
    protected final RedisReadWriteLock rrwl;

    /** Lock id */
    protected final long id;

    /** Lock flag */
    protected boolean is_locked = false;

    /** Abstract constructor
     *  @param rrwl Parent RedisReadWriteLock instance
     *  @throws IllegalArgumentException thrown when rrwl is null
     */
    protected GenericLock(final RedisReadWriteLock rrwl) throws IllegalArgumentException{

        // Check parameter
        if(rrwl == null) throw new IllegalArgumentException("'rrwl' parameter in GenericLock(RedisReadWriteLock) is null");

        // Assign parameter to class variable
        this.rrwl = rrwl;

        // Draw unique id number from counter TODO what happens if counter wraps
        this.id = ID_COUNTER.getAndIncrement();
    }

    /** Returns the state of lock
     *  @return true if the lock is locked, false otherwise
     */
    public boolean isLocked(){
        return this.is_locked;
    }

    /** Acquires the lock.
     *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#lock--">Lock.lock()</a>
     */
    public abstract void lock();

    /** Acquires the lock.
     *  @param lease_time lock lease time
     *  @param unit the time unit of the time argument
     *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#lock--">Lock.lock()</a>
     */
    public abstract void lock(final TimeUnit unit, final long lease_time);

    /** Acquires the lock unless the current thread is interrupted.
     *  @throws InterruptedException if the current thread is interrupted while acquiring the lock (and interruption of lock acquisition is supported)
     *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#lockInterruptibly--">Lock.lockInterruptibly()</a>
     */
    public abstract void lockInterruptibly() throws InterruptedException;

    /** Acquires the lock unless the current thread is interrupted.
     *  @param lease_time lock lease time
     *  @param unit the time unit of the time argument
     *  @throws InterruptedException if the current thread is interrupted while acquiring the lock (and interruption of lock acquisition is supported)
     *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#lockInterruptibly--">Lock.lockInterruptibly()</a>
     */
    public abstract void lockInterruptibly(final TimeUnit unit, final long lease_time) throws InterruptedException;

    /** Acquires the lock only if it is free at the time of invocation.
     *  @return true if the lock was acquired and false otherwise
     *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#tryLock--">Lock.tryLock()</a>
     */
    public abstract boolean tryLock();

    /** Acquires the lock only if it is free at the time of invocation.
     *  @param lease_time lock lease time
     *  @param unit the time unit of the time argument
     *  @return true if the lock was acquired and false otherwise
     *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#tryLock--">Lock.tryLock()</a>
     */
    public abstract boolean tryLock(final TimeUnit unit, final long lease_time);

    /** Acquires the lock if it is free within the given waiting time and the current thread has not been interrupted.
     *  @param time the maximum time to wait for the lock
     *  @param unit the time unit of the time argument
     *  @return true if the lock was acquired and false if the waiting time elapsed before the lock was acquired
     *  @throws InterruptedException if the current thread is interrupted while acquiring the lock (and interruption of lock acquisition is supported)
     *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#tryLock-long-java.util.concurrent.TimeUnit-">Lock.tryLock(long,TimeUnit)</a>
     */
    public abstract boolean tryLock(final long time, final TimeUnit unit) throws InterruptedException;

    /** Acquires the lock if it is free within the given waiting time and the current thread has not been interrupted.
     *  @param wait_time the maximum time to wait for the lock
     *  @param lease_time lock lease time
     *  @param unit the time unit of the time argument
     *  @return true if the lock was acquired and false if the waiting time elapsed before the lock was acquired
     *  @throws InterruptedException if the current thread is interrupted while acquiring the lock (and interruption of lock acquisition is supported)
     *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#tryLock-long-java.util.concurrent.TimeUnit-">Lock.tryLock(long,TimeUnit)</a>
     */
    public abstract boolean tryLock(final long wait_time, final TimeUnit unit, final long lease_time) throws InterruptedException;

    /** Performs lock() and returns the lock
     *  @return Locked lock
     */
    public Lock doLock(){
        this.lock();
        return this;
    }

    /** Performs lock() and returns the lock
     *  @param lease_time lock lease time
     *  @param unit the time unit of the time argument
     *  @return Locked lock
     */
    public Lock doLock(final TimeUnit unit, final long lease_time){
        this.lock(unit, lease_time);
        return this;
    }

    /** Performs lockInterruptibly() and returns the lock
     *  @return Locked lock
     *  @throws InterruptedException if the current thread is interrupted while acquiring the lock (and interruption of lock acquisition is supported)
     */
    public Lock doLockInterruptibly() throws InterruptedException{
        this.lockInterruptibly();
        return this;
    }

    /** Performs lockInterruptibly() and returns the lock
     *  @param lease_time lock lease time
     *  @param unit the time unit of the time argument
     *  @return Locked lock
     *  @throws InterruptedException if the current thread is interrupted while acquiring the lock (and interruption of lock acquisition is supported)
     */
    public Lock doLockInterruptibly(final TimeUnit unit, final long lease_time) throws InterruptedException{
        this.lockInterruptibly(unit, lease_time);
        return this;
    }

    /** Returns a new Condition instance that is bound to this Lock instance.
     *  @return A new Condition instance for this Lock instance
     *  @throws UnsupportedOperationException if the AbstractRedisLockClient does not support this
     *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#newCondition--">Lock.newCondition()</a>
     */
    public abstract Condition newCondition() throws UnsupportedOperationException;

    /** Releases the lock.
     *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#unlock--">Lock.unlock()</a>
     */
    public abstract void unlock();

    /** Releases the lock and closes any underlying resouces
     *  This method does <b>not</b> close RedisReadWriteLock or the associated AbstractRedisLockClient instance
     *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/lang/AutoCloseable.html#close--">AutoCloseable.close()</a>
     */
    public void close(){
        this.unlock();
    }
}
