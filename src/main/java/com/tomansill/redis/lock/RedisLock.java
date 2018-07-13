package com.tomansill.redis.lock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public interface RedisLock extends Lock{

    public boolean isLocked();

    public void lock(long lease_time, TimeUnit unit);

    public void lockInterruptibly(long lease_time, TimeUnit unit) throws InterruptedException;

    public boolean tryLock(long wait_time, long lease_time, TimeUnit unit) throws InterruptedException;

}
