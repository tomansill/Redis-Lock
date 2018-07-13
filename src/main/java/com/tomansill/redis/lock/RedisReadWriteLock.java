package com.tomansill.redis.lock;

import com.tomansill.redis.AbstractRedisClient;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.TimeUnit;

/** RedisReadWriteLock class
 *  @author Thomas Ansill
 *  @email tom@ansill.com
 */
public class RedisReadWriteLock implements ReadWriteLock{

    /* Redis Client */
    private AbstractRedisClient client;

    /* Lock point */
    private String lockpoint;

    /* fair flag */
    private boolean is_fair;

    /** Creates an instance of RedisReadWriteLock
     *  @param lockpoint lockpoint for this lock to claim on Redis server
     *  @param client Redis client
     *  @throws IllegalArgumentException thrown when either lockpoint or client parameter is null
     */
    public RedisReadWriteLock(String lockpoint, AbstractRedisClient client){
        this(lockpoint, client, false);
    }

    /** Creates an instance of RedisReadWriteLock
     *  @param lockpoint lockpoint for this lock to claim on Redis server
     *  @param client Redis client
     *  @throws IllegalArgumentException thrown when either lockpoint or client parameter is null
     *  @throws UnsupportedOperationException thrown when the given AbstractRedisClient instance does not support this fairness policy
     */
    public RedisReadWriteLock(String lockpoint, AbstractRedisClient client, boolean is_fair){

        // Check parameters
        if(lockpoint == null){
            throw new IllegalArgumentException("'lockpoint' parameter in RedisReadWriteLock(String, AbstractRedisClient, boolean) is null");
        }
        if(client == null){
            throw new IllegalArgumentException("'client' parameter in RedisReadWriteLock(String, AbstractRedisClient, boolean) is null");
        }

        // Check fairness policy
        if(is_fair && !client.isFairSupported()){
            throw new UnsupportedOperationException(client.getClass().getName() + " does not support fair locking.");
        }
        if(!is_fair && !client.isUnfairSupported()){
            throw new UnsupportedOperationException(client.getClass().getName() + " does not support unfair locking.");
        }

        // Assign parameters to class variables
        this.lockpoint = lockpoint;
        this.client = client;
        this.is_fair = is_fair;
    }

    /** Returns the client that this instance is using
     *  @return the Redis client
     */
    public AbstractRedisClient getClient(){
        return this.client;
    }

    /** Returns the lockpoint that this instance is using
     *  @return the lockpoint
     */
    public String getLockpoint(){
        return this.lockpoint;
    }

    /** Returns the fairness flag
     *  @return true if this RedisReadWriteLock is fair, false otherwise
     */
    public boolean isFair(){
        return this.is_fair;
    }

    /** Returns the lock used for reading
     *  @return the lock used for reading
     *  @throws UnsupportedOperationException thrown if AbstractRedisClient associated to the instance does not support read locks
     *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/ReadWriteLock.html#readLock--">ReadWriteLock.readLock()</a>
     */
    public Lock readLock(){

        // Check read lock support
        if(!this.client.isInclusiveLockSupported()){
            throw new UnsupportedOperationException(client.getClass().getName() + " does not support read locks.");
        }

        // Return ReadLock
        return new ReadLock(this);
    }

    /** Returns the lock used for writing
     *  @return the lock used for writing
     *  @throws UnsupportedOperationException thrown if AbstractRedisClient associated to the instance does not support write locks
     *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/ReadWriteLock.html#writeLock--">ReadWriteLock.writeLock()</a>
     */
    public Lock writeLock(){

        // Check write lock support
        if(!this.client.isExclusiveLockSupported()){
            throw new UnsupportedOperationException(client.getClass().getName() + " does not support write locks.");
        }

        // Return ReadLock
        return new WriteLock(this);
    }

    /** ReadLock class */
    public class ReadLock extends GenericLock{

        /** Creates ReadLock instance
         *  @param rrwl Parent RedisReadWriteLock instance
         *  @throws IllegalArgumentException thrown when rrwl is null
         */
        private ReadLock(RedisReadWriteLock rrwl){
            super(rrwl);
        }

        /** Acquires the lock.
         *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#lock--">Lock.lock()</a>
         */
        public void lock(){
            if(this.is_locked) return;
        }

        public void lock(long lease_time, TimeUnit unit){
            if(this.is_locked) return;
        }

        /** Acquires the lock unless the current thread is interrupted.
         *  @throws InterruptedException if the current thread is interrupted while acquiring the lock (and interruption of lock acquisition is supported)
         *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#lockInterruptibly--">Lock.lockInterruptibly()</a>
         */
        public void lockInterruptibly() throws InterruptedException{
            if(this.is_locked) return;
        }

        public void lockInterruptibly(long lease_time, TimeUnit unit) throws InterruptedException{
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

        /** Acquires the lock if it is free within the given waiting time and the current thread has not been interrupted.
         *  @param time the maximum time to wait for the lock
         *  @param unit the time unit of the time argument
         *  @return true if the lock was acquired and false if the waiting time elapsed before the lock was acquired
         *  @throws InterruptedException if the current thread is interrupted while acquiring the lock (and interruption of lock acquisition is supported)
         *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#tryLock-long-java.util.concurrent.TimeUnit-">Lock.tryLock(long,TimeUnit)</a>
         */
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException{
            if(this.is_locked) return true;
            return false;
        }

        public boolean tryLock(long wait_time, long lease_time, TimeUnit unit)  throws InterruptedException{
            if(this.is_locked) return true;
            return false;
        }

        /** Returns a new Condition instance that is bound to this Lock instance.
         *  @return A new Condition instance for this Lock instance
         *  @throws UnsupportedOperationException if the AbstractRedisClient does not support this
         *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#newCondition--">Lock.newCondition()</a>
         */
        public Condition newCondition() throws UnsupportedOperationException{
            if(!this.rrwl.getClient().isInclusiveNewConditionSupported()){
                throw new UnsupportedOperationException(this.rrwl.getClient().getClass().getName() + " does not support newCondition() for read locks");
            }
            return null;
        }

        public void unlock(){
            if(!this.is_locked) return;
        }
    }

    /** WriteLock class */
    public class WriteLock extends GenericLock{

        /** Creates WriteLock instance
         *  @param rrwl Parent RedisReadWriteLock instance
         *  @throws IllegalArgumentException thrown when rrwl is null
         */
        private WriteLock(RedisReadWriteLock rrwl){
            super(rrwl);
        }

        /** Acquires the lock.
         *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#lock--">Lock.lock()</a>
         */
        public void lock(){
            if(this.is_locked) return;
        }

        public void lock(long lease_time, TimeUnit unit){
            if(this.is_locked) return;
        }

        /** Acquires the lock unless the current thread is interrupted.
         *  @throws InterruptedException if the current thread is interrupted while acquiring the lock (and interruption of lock acquisition is supported)
         *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#lockInterruptibly--">Lock.lockInterruptibly()</a>
         */
        public void lockInterruptibly() throws InterruptedException{
            if(this.is_locked) return;
        }

        public void lockInterruptibly(long lease_time, TimeUnit unit) throws InterruptedException{
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

        /** Acquires the lock if it is free within the given waiting time and the current thread has not been interrupted.
         *  @param time the maximum time to wait for the lock
         *  @param unit the time unit of the time argument
         *  @return true if the lock was acquired and false if the waiting time elapsed before the lock was acquired
         *  @throws InterruptedException if the current thread is interrupted while acquiring the lock (and interruption of lock acquisition is supported)
         *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#tryLock-long-java.util.concurrent.TimeUnit-">Lock.tryLock(long,TimeUnit)</a>
         */
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException{
            if(this.is_locked) return true;
            return false;
        }

        public boolean tryLock(long wait_time, long lease_time, TimeUnit unit)  throws InterruptedException{
            if(this.is_locked) return true;
            return false;
        }

        /** Returns a new Condition instance that is bound to this Lock instance.
         *  @return A new Condition instance for this Lock instance
         *  @throws UnsupportedOperationException if the AbstractRedisClient does not support this
         *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#newCondition--">Lock.newCondition()</a>
         */
        public Condition newCondition() throws UnsupportedOperationException{
            if(!this.rrwl.getClient().isExclusiveNewConditionSupported()){
                throw new UnsupportedOperationException(this.rrwl.getClient().getClass().getName() + " does not support newCondition() for write locks");
            }
            return null;
        }

        /** Returns a new Condition instance that is bound to this Lock instance.
         *  @return A new Condition instance for this Lock instance
         *  @throws UnsupportedOperationException if the AbstractRedisClient does not support this
         *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#newCondition--">Lock.newCondition()</a>
         */
        public void unlock(){
            if(!this.is_locked) return;
        }
    }

    /** Generic Lock class that provides some implementation to ReadLock and WriteLock subclasses */
    private static abstract class GenericLock implements RedisLock, AutoCloseable{

        /** Parent RedisReadWriteLock instance */
        protected RedisReadWriteLock rrwl;

        /** Lock flag */
        protected boolean is_locked = false;

        /** Abstract constructor
         *  @param rrwl Parent RedisReadWriteLock instance
         *  @throws IllegalArgumentException thrown when rrwl is null
         */
        protected GenericLock(RedisReadWriteLock rrwl) throws IllegalArgumentException{

            // Check parameter
            if(rrwl == null) throw new IllegalArgumentException("'rrwl' parameter in GenericLock(RedisReadWriteLock) is null");

            // Assign parameter to class variable
            this.rrwl = rrwl;
        }

        public boolean isLocked(){
            return this.is_locked;
        }

        /** Acquires the lock.
         *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#lock--">Lock.lock()</a>
         */
        public abstract void lock();

        public abstract void lock(long lease_time, TimeUnit unit);

        /** Acquires the lock unless the current thread is interrupted.
         *  @throws InterruptedException if the current thread is interrupted while acquiring the lock (and interruption of lock acquisition is supported)
         *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#lockInterruptibly--">Lock.lockInterruptibly()</a>
         */
        public abstract void lockInterruptibly() throws InterruptedException;

        public abstract void lockInterruptibly(long lease_time, TimeUnit unit) throws InterruptedException;

        /** Acquires the lock only if it is free at the time of invocation.
         *  @return true if the lock was acquired and false otherwise
         *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#tryLock--">Lock.tryLock()</a>
         */
        public abstract boolean tryLock();

        /** Acquires the lock if it is free within the given waiting time and the current thread has not been interrupted.
         *  @param time the maximum time to wait for the lock
         *  @param unit the time unit of the time argument
         *  @return true if the lock was acquired and false if the waiting time elapsed before the lock was acquired
         *  @throws InterruptedException if the current thread is interrupted while acquiring the lock (and interruption of lock acquisition is supported)
         *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#tryLock-long-java.util.concurrent.TimeUnit-">Lock.tryLock(long,TimeUnit)</a>
         */
        public abstract boolean tryLock(long time, TimeUnit unit) throws InterruptedException;

        public abstract boolean tryLock(long wait_time, long lease_time, TimeUnit unit) throws InterruptedException;

        /** Returns a new Condition instance that is bound to this Lock instance.
         *  @return A new Condition instance for this Lock instance
         *  @throws UnsupportedOperationException if the AbstractRedisClient does not support this
         *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#newCondition--">Lock.newCondition()</a>
         */
        public abstract Condition newCondition() throws UnsupportedOperationException;

        /** Releases the lock.
         *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html#unlock--">Lock.unlock()</a>
         */
        public abstract void unlock();

        /** Releases the lock and closes any underlying resouces
         *  This method does <b>not</b> close RedisReadWriteLock or the associated AbstractRedisClient instance
         *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/lang/AutoCloseable.html#close--">AutoCloseable.close()</a>
         */
        public void close(){
            this.unlock();
        }
    }
}
