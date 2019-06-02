package com.ansill.redis.lock;

import com.ansill.lock.autolock.LockedAutoLock;

import javax.annotation.Nonnull;
import java.util.concurrent.locks.Lock;

/** LockedAutoLock implementation */
public class LockedRedisLock implements LockedAutoLock{

    /** Lock object */
    @Nonnull
    private final Lock lock;

    /**
     * Creates locked resource
     *
     * @param lock lock
     */
    LockedRedisLock(@Nonnull Lock lock){
        this.lock = lock;
    }

    @Override
    public void unlock(){
        this.lock.unlock();
    }
}
