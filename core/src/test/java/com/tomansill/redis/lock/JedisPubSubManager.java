package com.tomansill.redis.lock;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.net.SocketException;
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

	public JedisPubSubManager(final Jedis jedis){
		this.connection = jedis;
	}

	public boolean subscribe(final String channel, final Consumer<String> function){

		// Check input
		if(channel == null) throw new IllegalArgumentException("channel is null");
		if(channel.isEmpty()) throw new IllegalArgumentException("channel is empty");
		if(function == null) throw new IllegalArgumentException("function is null");

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

				// Run it (subscribe is a blocking function so new thread is needed)
				new Thread(() -> {
					try{
						this.connection.subscribe(this.pubsub, channel);
					}catch(JedisConnectionException e){
						if(e.getCause() instanceof SocketException) closed = true;
					}catch(ClassCastException e){
						// We can safely ignore this, I think this is Jedis error with socket being closed
						// TODO on future versions
						closed = true;
					}
				}).start();


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

	public boolean unsubscribe(final String channel){
		Lock lock = this.rwl.writeLock();
		lock.lock();
		try{
			return this.unsubscribeWithoutLock(channel);
		}finally{
			lock.unlock();
		}
	}

	private boolean unsubscribeWithoutLock(final String channel){

		// Check if closed
		if(this.closed) return false;

		// Check if channel exists (and remove it if it exists)
		if(this.channel_function_map.remove(channel) == null) return false;

		// Skip if somehow already unsubscribe TODO validity of this statement
		if(this.pubsub == null) return true;

		// Unsubscribe
		this.pubsub.unsubscribe(channel);

		// Stop the pubsub if all empty
		if(!this.channel_function_map.isEmpty()) return true;

		// Nullify it (to kill it fully) TODO validtiy of this statement
		this.pubsub = null;

		// Exit
		return true;
	}

	private void unsubscribeAllWithoutLock(){
		// Check if closed
		if(this.closed) return;

		// Loop all
		for(String channel : this.channel_function_map.keySet()){
			this.unsubscribe(channel);
		}
	}

	public synchronized void unsubscribeAll(){
		Lock lock = this.rwl.writeLock();
		lock.lock();
		try{
			this.unsubscribeAllWithoutLock();
		}finally{
			lock.unlock();
		}
	}

	@Override
	public void close(){

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

	private class PubSub extends JedisPubSub{
		@Override
		public void onMessage(final String channel, final String message){

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
