package com.ansill.redis.lock;

import com.ansill.lock.autolock.LockedAutoLock;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;

/** Generic Lock class that provides some implementation to ReadLock and WriteLock subclasses */
abstract class GenericLock implements AutoRedisLock, RedisLock{

    /** ID Counter */
    private final static AtomicLong ID_COUNTER = new AtomicLong();

    /** Parent RedisReadWriteLock instance */
    protected final RedisReadWriteLock rrwl;

    /** Lock id */
    protected final long id;

    /** Lock flag */
    protected final AtomicBoolean is_locked = new AtomicBoolean(false);

    /** Is Read Lock */
    private final boolean is_read_lock;

    /** Abstract constructor
     *  @param rrwl Parent RedisReadWriteLock instance
     *  @param is_read_lock
     * @throws IllegalArgumentException thrown when rrwl is null
     */
    protected GenericLock(@Nonnull RedisReadWriteLock rrwl, boolean is_read_lock) throws IllegalArgumentException{

        // Assign parameter to class variable
        this.rrwl = rrwl;
        this.is_read_lock = is_read_lock;

        // Draw unique id number from counter TODO what happens if counter wraps
        this.id = ID_COUNTER.getAndIncrement();
    }

    /** Performs lock() and returns the lock
     *  @return Locked lock
     */
    @Nonnull
    @Override
    public LockedAutoLock doLock(){
        this.lock();
        return new LockedRedisLock(this);
    }

    /** Performs lock() and returns the lock
     *  @param lease_time lock lease time
     *  @param unit the time unit of the time argument
     *  @return Locked lock
     */
    @Nonnull
    @Override
    public LockedAutoLock doLock(@Nonnull TimeUnit unit, @Nonnegative long lease_time){
        this.lock(unit, lease_time);
        return new LockedRedisLock(this);
    }

    /** Performs lock() and returns the lock
     *  @return Locked lock
     */
    @Nonnull
    @Override
    public LockedAutoLock doTryLock(){
        this.lock();
        return new LockedRedisLock(this);
    }

    /** Performs lock() and returns the lock
     *  @param time time to try the lock
     *  @param unit the time unit of the time argument
     *  @return Locked lock
     */
    @Nonnull
    @Override
    public LockedAutoLock doTryLock(@Nonnegative long time, @Nonnull TimeUnit unit) throws InterruptedException{
        this.tryLock(time, unit);
        return new LockedRedisLock(this);
    }

    /** Performs lockInterruptibly() and returns the lock
     *  @return Locked lock
     *  @throws InterruptedException if the current thread is interrupted while acquiring the lock (and interruption of lock acquisition is supported)
     */
    @Nonnull
    @Override
    public LockedAutoLock doLockInterruptibly() throws InterruptedException{
        this.lockInterruptibly();
        return new LockedRedisLock(this);
    }

    /** Performs lockInterruptibly() and returns the lock
     *  @param lease_time lock lease time
     *  @param unit the time unit of the time argument
     *  @return Locked lock
     *  @throws InterruptedException if the current thread is interrupted while acquiring the lock (and interruption of lock acquisition is supported)
     */
    @Nonnull
    @Override
    public LockedAutoLock doLockInterruptibly(@Nonnull TimeUnit unit, @Nonnegative long lease_time)
    throws InterruptedException{
        this.lockInterruptibly(unit, lease_time);
        return new LockedRedisLock(this);
    }

    @Nonnull
    @Override
    public LockedAutoLock doTryLock(@Nonnull TimeUnit unit, long lease_time) throws TimeoutException{
        if(!this.tryLock(unit, lease_time)) throw new TimeoutException("Timed out!");
        return new LockedRedisLock(this);
    }

    @Nonnull
    @Override
    public LockedAutoLock doTryLock(long wait_time, @Nonnull TimeUnit unit, long lease_time)
    throws TimeoutException, InterruptedException{
        if(!this.tryLock(wait_time, unit, lease_time)) throw new TimeoutException("Timed out!");
        return new LockedRedisLock(this);
    }

    /**
     * Returns a new Condition instance that is bound to this Lock instance.
     *
     * @return A new Condition instance for this Lock instance
     * @throws UnsupportedOperationException if the RedisClient does not support this
     * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#newCondition--">Lock.newCondition()</a>
     */
    @Nonnull
    public final Condition newCondition() throws UnsupportedOperationException{
        throw new UnsupportedOperationException(this.getClass().getName() +
                                                " does not support newCondition()");
    }

    /**
     * Releases the lock.
     *
     * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#unlock--">Lock.unlock()</a>
     */
    @Override
    public final void unlock(){

        // Short circuit
        if(!this.is_locked.get()) return;

        // Unlock
        this.rrwl.getClient().unlock(this.rrwl.getLockpoint(), this.id + "", this.is_read_lock);

        // Update flag
        this.is_locked.set(false);
    }

    @Override
    public final boolean isLocked(){
        return this.is_locked.get();
    }
}
