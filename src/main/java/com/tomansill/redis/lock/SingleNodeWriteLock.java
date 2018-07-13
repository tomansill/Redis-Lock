package com.tomansill.redis.lock;

import com.tomansill.redis.AbstractRedisClient;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.TimeUnit;

/** SingleNodeWriteLock class */
class SingleNodeWriteLock extends GenericLock{

    /** Creates WriteLock instance
     *  @param rrwl Parent RedisReadWriteLock instance
     *  @throws IllegalArgumentException thrown when rrwl is null
     */
    SingleNodeWriteLock(final RedisReadWriteLock rrwl, final boolean is_fair){
        super(rrwl, is_fair);
    }

    /** Acquires the lock.
     *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#lock--">Lock.lock()</a>
     */
    public void lock(){
        if(this.is_locked) return;
    }

    /** Acquires the lock.
     *  @param wait_time the maximum time to wait for the lock
     *  @param lease_time lock lease time
     *  @param unit the time unit of the time argument
     *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#lock--">Lock.lock()</a>
     */
    public void lock(final TimeUnit unit, final long lease_time){
        if(this.is_locked) return;
    }

    /** Acquires the lock unless the current thread is interrupted.
     *  @throws InterruptedException if the current thread is interrupted while acquiring the lock (and interruption of lock acquisition is supported)
     *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#lockInterruptibly--">Lock.lockInterruptibly()</a>
     */
    public void lockInterruptibly() throws InterruptedException{
        if(this.is_locked) return;
    }

    /** Acquires the lock unless the current thread is interrupted.
     *  @param lease_time lock lease time
     *  @param unit the time unit of the time argument
     *  @throws InterruptedException if the current thread is interrupted while acquiring the lock (and interruption of lock acquisition is supported)
     *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#lockInterruptibly--">Lock.lockInterruptibly()</a>
     */
    public void lockInterruptibly(final TimeUnit unit, final long lease_time) throws InterruptedException{
        if(this.is_locked) return;
    }

    /** Acquires the lock if it is free within the given waiting time and the current thread has not been interrupted.
     *  @return true if the lock was acquired and false if the waiting time elapsed before the lock was acquired
     *  @throws InterruptedException if the current thread is interrupted while acquiring the lock (and interruption of lock acquisition is supported)
     *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#tryLock-long-java.util.concurrent.TimeUnit-">Lock.tryLock(long,TimeUnit)</a>
     */
    public boolean tryLock(){
        if(this.is_locked) return true;
        return false;
    }

    /** Acquires the lock only if it is free at the time of invocation.
     *  @param lease_time lock lease time
     *  @param unit the time unit of the time argument
     *  @return true if the lock was acquired and false otherwise
     *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#tryLock--">Lock.tryLock()</a>
     */
    public boolean tryLock(final TimeUnit unit, final long lease_time){
        if(this.is_locked) return true;
        return false;
    }

    /** Acquires the lock if it is free within the given waiting time and the current thread has not been interrupted.
     *  @param time the maximum time to wait for the lock
     *  @param unit the time unit of the time argument
     *  @return true if the lock was acquired and false if the waiting time elapsed before the lock was acquired
     *  @throws InterruptedException if the current thread is interrupted while acquiring the lock (and interruption of lock acquisition is supported)
     *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#tryLock-long-java.util.concurrent.TimeUnit-">Lock.tryLock(long,TimeUnit)</a>
     */
    public boolean tryLock(final long time, final TimeUnit unit) throws InterruptedException{
        if(this.is_locked) return true;
        return false;
    }

    /** Acquires the lock if it is free within the given waiting time and the current thread has not been interrupted.
     *  @param wait_time the maximum time to wait for the lock
     *  @param lease_time lock lease time
     *  @param unit the time unit of the time argument
     *  @return true if the lock was acquired and false if the waiting time elapsed before the lock was acquired
     *  @throws InterruptedException if the current thread is interrupted while acquiring the lock (and interruption of lock acquisition is supported)
     *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#tryLock-long-java.util.concurrent.TimeUnit-">Lock.tryLock(long,TimeUnit)</a>
     */
    public boolean tryLock(final long wait_time, final TimeUnit unit, final long lease_time)  throws InterruptedException{
        if(this.is_locked) return true;
        return false;
    }

    /** Returns a new Condition instance that is bound to this Lock instance.
     *  @return A new Condition instance for this Lock instance
     *  @throws UnsupportedOperationException if the RedisClient does not support this
     *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#newCondition--">Lock.newCondition()</a>
     */
    public Condition newCondition() throws UnsupportedOperationException{
        if(!this.rrwl.getClient().isExclusiveNewConditionSupported()){
            throw new UnsupportedOperationException(this.rrwl.getClient().getClass().getName() + " does not support newCondition() for write locks");
        }
        return null;
    }

    /** Releases the lock.
     *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#unlock--">Lock.unlock()</a>
     */
    public void unlock(){
        if(!this.is_locked) return;
    }
}
