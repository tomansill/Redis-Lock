package com.tomansill.redis.lock;

import java.util.concurrent.locks.ReadWriteLock;

/** RedisReadWriteLock class
 *  The ReadWriteLock implements ReadWriteLock's methods and returns read and write locks.
 *  @author <a href="mailto:tom@ansill.com">Tom Ansill</a>
 */
public class RedisReadWriteLock implements ReadWriteLock{

    /* Redis Client */
    private final AbstractRedisLockClient client;

    /* Lock point */
    private final String lockpoint;

    /* fair flag */
    private final boolean is_fair;

    /** Creates an instance of RedisReadWriteLock
     *  @param lockpoint lockpoint for this lock to claim on Redis server
     *  @param client Redis client
     *  @throws IllegalArgumentException thrown when either lockpoint or client parameter is null
     */
    RedisReadWriteLock(final String lockpoint, final AbstractRedisLockClient client){
        this(lockpoint, client, false);
    }

    /** Creates an instance of RedisReadWriteLock
     *  @param lockpoint lockpoint for this lock to claim on Redis server
     *  @param client Redis client
     *  @param is_fair true to enforce fair locking order, false to let locks to acquire in unspecified way
     *  @throws IllegalArgumentException thrown when either lockpoint or client parameter is null
     *  @throws UnsupportedOperationException thrown when the given AbstractRedisLockClient instance does not support this fairness policy
     */
    RedisReadWriteLock(final String lockpoint, final AbstractRedisLockClient client, final boolean is_fair){

        // Check parameters
        if(lockpoint == null){
            throw new IllegalArgumentException("'lockpoint' parameter in RedisReadWriteLock(String, AbstractRedisLockClient, boolean) is null");
        }
        if(client == null){
            throw new IllegalArgumentException("'client' parameter in RedisReadWriteLock(String, AbstractRedisLockClient, boolean) is null");
        }

        // Assign parameters to class variables
        this.lockpoint = lockpoint;
        this.client = client;
        this.is_fair = is_fair;
    }

    /** Returns the client that this instance is using
     *  @return the Redis client
     */
    public AbstractRedisLockClient getClient(){
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
     *  @throws UnsupportedOperationException thrown if AbstractRedisLockClient associated to the instance does not support read locks
     *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/ReadWriteLock.html#readLock--">ReadWriteLock.readLock()</a>
     */
    public AutoCloseableRedisLock readLock(){

        // Return ReadLock
        if(this.client.isCluster()) return null;
        else return new SingleNodeReadLock(this);
    }

    /** Returns the lock used for writing
     *  @return the lock used for writing
     *  @throws UnsupportedOperationException thrown if AbstractRedisLockClient associated to the instance does not support write locks
     *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/ReadWriteLock.html#writeLock--">ReadWriteLock.writeLock()</a>
     */
    public AutoCloseableRedisLock writeLock(){

        // Return WriteLock
        if(this.client.isCluster()) return null;
        else return new SingleNodeWriteLock(this);
    }
}
