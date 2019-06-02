package com.ansill.redis.test;

import com.tomansill.old.redis.test.util.ResetableCountDownLatch;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/** Simple blocking channel */
public class MessageQueue{

    /** Queue */
    @Nonnull
    private final Queue<String> queue = new ConcurrentLinkedQueue<>();

    /** Resettable CountDownLatch */
    @Nonnull
    private final ResetableCountDownLatch rcdl = new ResetableCountDownLatch(1);

    /** Default constructor */
    public MessageQueue(){

    }

    public synchronized void enqueue(@Nonnull String message){
        this.queue.add(message);
        this.rcdl.countDown();
    }

    @Nonnull
    public Optional<String> poll(@Nonnegative long time, @Nonnull TimeUnit unit)
    throws InterruptedException{

        // Wait for it
        if(!this.rcdl.await(time, unit)) return Optional.empty();

        // Synchronize
        synchronized(this){

            // Get item
            String item = this.queue.poll();

            // If queue is empty, reset the countdownlatch
            if(item == null || this.queue.isEmpty()) this.rcdl.reset(1);

            // Return it
            return Optional.ofNullable(item);
        }
    }
}