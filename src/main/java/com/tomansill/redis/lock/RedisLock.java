package com.tomansill.redis.lock;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.TimeUnit;

public class RedisLock extends ReadWriteLock{

    private RedisClient client;

    public RedisLock(RedisClient client){
        this.client = client;
    }

    public getClient(){
        this.client;
    }

    public class ReadLock extends GenericLock{

        private ReadLock(){}

    }

    public class WriteLock extends GenericLock{

        private WriteLock(){}

    }

    private class GenericLock implements Lock, AutoCloseable{

        public abstract void lock();

        public abstract void lockInterruptibly() throws InterruptedException;

        public abstract boolean tryLock();

        public abstract boolean tryLock(long time, TimeUnit unit) throws InterruptedException;

        public abstract void unlock();

        public Condition newCondition() throws UnsupportedOperationException{
            throw new UnsupportedOperationException("Not supported");
        }

        public void close(){
            this.unlock();
        }
    }
}
