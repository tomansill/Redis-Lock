package com.ansill.redis.lock;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

/** RedisWriteLock class */
class RedisWriteLock extends GenericLock implements AutoRedisLock{

    /**
     * Creates WriteLock instance
     *
     * @param rrwl Parent RedisReadWriteLock instance
     * @throws IllegalArgumentException thrown when rrwl is null
     */
    RedisWriteLock(@Nonnull RedisReadWriteLock rrwl){
        super(rrwl, true);
    }

    /**
     * Acquires the lock.
     *
     * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#lock--">Lock.lock()</a>
     */
    public void lock(){

        // Call it
        this.innerLock(null, 0);
    }

    /**
     * Acquires the lock.
     *
     * @param lease_time lock lease time
     * @param unit       the time unit of the time argument
     * @throws IllegalArgumentException thrown if unit or lease_time is invalid
     * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#lock--">Lock.lock()</a>
     */
    public void lock(@Nonnull TimeUnit unit, @Nonnegative long lease_time) throws IllegalArgumentException{

        // Check parameter
        if(lease_time <= 0) throw new IllegalArgumentException("lease_time parameter is below the minimum value of 1");

        // Call it
        this.innerLock(unit, lease_time);
    }

    /**
     * Internal function for locking
     *
     * @param lease_time lock lease time
     * @param unit       the time unit of the time argument
     * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#lock--">Lock.lock()</a>
     */
    private void innerLock(@Nullable TimeUnit unit, @Nonnegative long lease_time){
        try{
            this.innerLockInterruptibly(unit, lease_time);
        }catch(InterruptedException e){
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Acquires the lock unless the current thread is interrupted.
     *
     * @throws InterruptedException if the current thread is interrupted while acquiring the lock (and interruption of lock acquisition is supported)
     * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#lockInterruptibly--">Lock.lockInterruptibly()</a>
     */
    public void lockInterruptibly() throws InterruptedException{

        // Call it
        this.innerLockInterruptibly(null, 0);
    }

    /**
     * Inner function of lockInterruptibly. Acquires the lock unless the current thread is interrupted.
     *
     * @param lease_time lock lease time
     * @param unit       the time unit of the time argument
     * @throws IllegalArgumentException thrown if unit or lease_time is invalid
     * @throws InterruptedException     if the current thread is interrupted while acquiring the lock (and interruption of lock acquisition is supported)
     * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#lockInterruptibly--">Lock.lockInterruptibly()</a>
     */
    public void lockInterruptibly(@Nonnull TimeUnit unit, @Nonnegative long lease_time) throws InterruptedException{

        // Check parameter
        if(lease_time <= 0) throw new IllegalArgumentException("lease_time parameter is below the minimum value of 1");

        // Call it
        this.innerLockInterruptibly(unit, lease_time);
    }

    /**
     * Acquires the lock unless the current thread is interrupted.
     *
     * @param lease_time lock lease time
     * @param unit       the time unit of the time argument
     * @throws InterruptedException if the current thread is interrupted while acquiring the lock (and interruption of lock acquisition is supported)
     * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#lockInterruptibly--">Lock.lockInterruptibly()</a>
     */
    private void innerLockInterruptibly(@Nullable TimeUnit unit, @Nonnegative long lease_time)
    throws InterruptedException{

        //System.out.println("RedisWriteLock::innerLockInterruptibly() id= " + this.id + " timeunit=" + unit + " lease_time=" + lease_time);

        // Short circuit
        if(this.is_locked.get()) return;

        // Lock it
        if(unit == null){
            this.is_locked.set(
                    this.rrwl.getClient().performLock(
                            this.rrwl.getLockpoint(),
                            this.id + "",
                            false,
                            false,
                            this.rrwl.isFair(),
                            -1,
                            null
                    ));
        }else{
            this.is_locked.set(
                    this.rrwl.getClient().performLock(
                            this.rrwl.getLockpoint(),
                            this.id + "",
                            false,
                            false,
                            this.rrwl.isFair(),
                            -1,
                            unit,
                            lease_time
                    )
            );
        }
    }

    /**
     * Acquires the lock if it is free within the given waiting time and the current thread has not been interrupted.
     *
     * @return true if the lock was acquired and false if the waiting time elapsed before the lock was acquired
     * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#tryLock-long-java.util.concurrent.TimeUnit-">Lock.tryLock(long,TimeUnit)</a>
     */
    public boolean tryLock(){
        try{
            return this.innerTryLock(0, TimeUnit.MILLISECONDS, 0);
        }catch(InterruptedException e){
            Thread.currentThread().interrupt();
        }
        return false;
    }

    /**
     * Acquires the lock only if it is free at the time of invocation.
     *
     * @param lease_time lock lease time
     * @param unit       the time unit of the time argument
     * @return true if the lock was acquired and false otherwise
     * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#tryLock--">Lock.tryLock()</a>
     */
    public boolean tryLock(@Nonnull TimeUnit unit, @Nonnegative long lease_time){

        // Check parameters
        if(lease_time <= 0) throw new IllegalArgumentException("lease_time parameter is below the minimum value of 1");

        // Lock it
        try{
            return this.innerTryLock(0, unit, lease_time);
        }catch(InterruptedException e){
            Thread.currentThread().interrupt();
        }
        return false;
    }

    /**
     * Acquires the lock if it is free within the given waiting time and the current thread has not been interrupted.
     *
     * @param time the maximum time to wait for the lock
     * @param unit the time unit of the time argument
     * @return true if the lock was acquired and false if the waiting time elapsed before the lock was acquired
     * @throws InterruptedException if the current thread is interrupted while acquiring the lock (and interruption of lock acquisition is supported)
     * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#tryLock-long-java.util.concurrent.TimeUnit-">Lock.tryLock(long,TimeUnit)</a>
     */
    public boolean tryLock(@Nonnegative long time, @Nonnull TimeUnit unit) throws InterruptedException{

        // Check parameters
        if(time <= 0) throw new IllegalArgumentException("time parameter is below the minimum value of 1");

        // Lock it
        return this.innerTryLock(time, unit, 0);
    }

    /**
     * Acquires the lock if it is free within the given waiting time and the current thread has not been interrupted.
     *
     * @param wait_time  the maximum time to wait for the lock
     * @param lease_time lock lease time
     * @param unit       the time unit of the time argument
     * @return true if the lock was acquired and false if the waiting time elapsed before the lock was acquired
     * @throws InterruptedException if the current thread is interrupted while acquiring the lock (and interruption of lock acquisition is supported)
     * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#tryLock-long-java.util.concurrent.TimeUnit-">Lock.tryLock(long,TimeUnit)</a>
     */
    public boolean tryLock(@Nonnegative long wait_time, @Nonnull TimeUnit unit, @Nonnegative long lease_time)
    throws InterruptedException{

        // Check parameters
        if(wait_time <= 0) throw new IllegalArgumentException("wait_time parameter is below the minimum value of 1");
        if(lease_time <= 0) throw new IllegalArgumentException("wait_time parameter is below the minimum value of 1");

        // Lock it
        return this.innerTryLock(wait_time, unit, lease_time);
    }

    private boolean innerTryLock(@Nonnegative long wait_time, @Nullable TimeUnit unit, @Nonnegative long lease_time)
    throws InterruptedException{

        // Short circuit
        if(this.is_locked.get()) return true;

        // Lock it
        if(unit == null){
            this.is_locked.set(
                    this.rrwl.getClient().performLock(
                            this.rrwl.getLockpoint(),
                            this.id + "",
                            false,
                            true,
                            this.rrwl.isFair(),
                            wait_time,
                            null
                    )
            );
        }else{
            this.is_locked.set(
                    this.rrwl.getClient().performLock(
                            this.rrwl.getLockpoint(),
                            this.id + "",
                            false,
                            true,
                            this.rrwl.isFair(),
                            wait_time,
                            unit,
                            lease_time
                    )
            );
        }

        return this.is_locked.get();
    }

}