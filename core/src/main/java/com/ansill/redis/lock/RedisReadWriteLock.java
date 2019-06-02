package com.ansill.redis.lock;

import javax.annotation.Nonnull;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/** RedisReadWriteLock class
 *  The ReadWriteLock implements ReadWriteLock's methods and returns read and write locks.
 *  @author <a href="mailto:tom@ansill.com">Tom Ansill</a>
 */
public class RedisReadWriteLock implements ReadWriteLock{

    /** Redis Client */
    @Nonnull
    private final AbstractRedisLockClient client;

    /** Lock point */
    @Nonnull
    private final String lockpoint;

    /** fair flag */
    private final boolean is_fair;

    /** Creates an instance of RedisReadWriteLock
     *  @param lockpoint lockpoint for this lock to claim on Redis server
     *  @param client Redis client
     *  @throws IllegalArgumentException thrown when either lockpoint or client parameter is null
     */
    RedisReadWriteLock(@Nonnull String lockpoint, @Nonnull AbstractRedisLockClient client) {
        this(lockpoint, client, false);
    }

    /** Creates an instance of RedisReadWriteLock
     *  @param lockpoint lockpoint for this lock to claim on Redis server
     *  @param client Redis client
     *  @param is_fair true to enforce fair locking order, false to let locks to acquire in unspecified way
     *  @throws IllegalArgumentException thrown when either lockpoint or client parameter is null
     *  @throws UnsupportedOperationException thrown when the given AbstractRedisLockClient instance does not support this fairness policy
     */
    RedisReadWriteLock(@Nonnull String lockpoint, @Nonnull AbstractRedisLockClient client, boolean is_fair) {
        this.lockpoint = lockpoint;
        this.client = client;
        this.is_fair = is_fair;
    }

    /** Returns the client that this instance is using
     *  @return the Redis client
     */
    @Nonnull
    public AbstractRedisLockClient getClient(){
        return this.client;
    }

    /** Returns the lockpoint that this instance is using
     *  @return the lockpoint
     */
    @Nonnull
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
     *  @throws UnsupportedOperationException thrown if AbstractRedisLockClient associated to the instance does not support read locks
     *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/ReadWriteLock.html#readLock--">ReadWriteLock.readLock()</a>
     */
    @Nonnull
    @Override
    public Lock readLock(){
        return new RedisReadLock(this);
    }

    /** Returns the lock used for writing
     *  @return the lock used for writing
     *  @throws UnsupportedOperationException thrown if AbstractRedisLockClient associated to the instance does not support write locks
     *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/ReadWriteLock.html#writeLock--">ReadWriteLock.performLock()</a>
     */
    @Nonnull
    @Override
    public Lock writeLock(){
        return new RedisWriteLock(this);
    }
}
