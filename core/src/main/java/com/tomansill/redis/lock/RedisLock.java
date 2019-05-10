package com.tomansill.redis.lock;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/** RedisLock interface
 *  This interface implements Lock and defines additional methods including lock lease times
 *  @author <a href="mailto:tom@ansill.com">Tom Ansill</a>
 */
public interface RedisLock extends Lock{

    boolean isLocked();

    void lock(@Nonnull TimeUnit unit, @Nonnegative long lease_time);

    void lockInterruptibly(@Nonnull TimeUnit unit, @Nonnegative long lease_time) throws InterruptedException;

    boolean tryLock(@Nonnull TimeUnit unit, @Nonnegative long lease_time);

    boolean tryLock(@Nonnegative long wait_time, @Nonnull TimeUnit unit, @Nonnegative long lease_time) throws InterruptedException;

}
