package com.tomansill.redis.jedis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

public class JedisPubSubManager implements AutoCloseable{

	private final Jedis connection;

	private final ConcurrentHashMap<String, Consumer<String>> channel_function_map = new ConcurrentHashMap<>();

	private PubSub pubsub = null;

	private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock(true);

	private boolean closed = false;

	public JedisPubSubManager(@Nonnull Jedis jedis) {
		this.connection = jedis;
	}

	public void subscribe(@Nonnull String channel, @Nonnull Consumer<String> function) {

		// Check input
		if(channel.isEmpty()) throw new IllegalArgumentException("channel is empty");

		boolean old_found = false;

		// Lock it
		Lock lock = this.rwl.writeLock();
		lock.lock();
		try{

			// Assert that this manager is still open
			if(closed) throw new IllegalStateException("JedisPubSubManager is closed");

			// Check if already subscribing
			if(this.channel_function_map.containsKey(channel)){

				// Unsubscribe
				this.unsubscribeWithoutLock(channel);

				// Remove old channel
				this.channel_function_map.remove(channel);

				// Set flag
				old_found = true;
			}

			// Add it
			this.channel_function_map.put(channel, function);

			// If pubsub is not running, create new and run it, otherwise add to existing pubsub
			if(this.pubsub == null){

				// Create pubsub
				this.pubsub = new PubSub();

				// Create new listener
				this.pubsub = new CustomPubSub(channel_function_map);

				// No other way to find out if listener is ready for receiving without polling the listener
				while(!this.pubsub.isSubscribed()){
					try{
						Thread.sleep(2);
					}catch(InterruptedException ignored){
					}
				}

			}else{
				this.pubsub.subscribe(channel);
			}

		}finally{
			lock.unlock();
		}

		return old_found;
	}

	public void unsubscribe(@Nonnull String channel) {

		// Check input
		if(channel.isEmpty()) throw new IllegalArgumentException("channel is empty");

		// Lock it
		Lock lock = this.rwl.writeLock();
		lock.lock();
		try{

			// Close it
			closed = true;

			// Unsubscribe all
			this.unsubscribeAllWithoutLock();

			// Close connection
			this.connection.close();

		}finally{

			// Unlock it
			lock.unlock();
		}

	}

	private static class CustomPubSub extends JedisPubSub {

		private final ConcurrentHashMap<String, Consumer<String>> channel_function_map;

		private CustomPubSub(@Nonnull ConcurrentHashMap<String, Consumer<String>> channel_function_map) {
			this.channel_function_map = channel_function_map;
		}

		@Override
		public void onMessage(@Nullable String channel, @Nullable String message) {

			// If null, ignore
			if(channel == null) return;

			// Lock it
			Lock lock = rwl.readLock();
			lock.lock();
			try{

				// Get function
				Consumer<String> function = channel_function_map.get(channel);

				// Error on unrecognized function
				if(function == null){
					System.err.println("Unrecognized channel name '" + channel + "' showed up in PubSub onMessage method");
					return;
				}

				// Run function
				function.accept(message);

			}finally{
				lock.unlock();
			}
		}
	}
}
