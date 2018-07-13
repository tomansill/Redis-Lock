package com.tomansill.redis.lock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/** RedisLock interface
 *  This interface implements Lock and defines additional methods including lock lease times
 *  @author <a href="mailto:tom@ansill.com">Tom Ansill</a>
 */
public interface RedisLock extends Lock{

    public boolean isLocked();

    public void lock(TimeUnit unit, long lease_time);

    public void lockInterruptibly(TimeUnit unit, long lease_time) throws InterruptedException;

    public boolean tryLock(TimeUnit unit, long lease_time);

    public boolean tryLock(long wait_time, TimeUnit unit, long lease_time) throws InterruptedException;

}
