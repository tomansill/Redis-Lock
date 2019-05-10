package com.tomansill.redis.jedis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class JedisPubSubManager{

	private final Jedis connection;

	private final ConcurrentHashMap<String, Consumer<String>> channel_function_map = new ConcurrentHashMap<>();

	private CustomPubSub pubsub = null;

	private final ReentrantLock lock = new ReentrantLock(true);

	public JedisPubSubManager(@Nonnull Jedis jedis) {
		this.connection = jedis;
	}

	public void subscribe(@Nonnull String channel, @Nonnull Consumer<String> function) {

		// Check input
		if(channel.isEmpty()) throw new IllegalArgumentException("channel is empty");

		// Lock it
		lock.lock();
		try{
			// Add in the map
			this.channel_function_map.put(channel, function);

			// Subscribe if listener already exists
			if(this.pubsub != null) this.pubsub.subscribe(channel);

			else{

				// Create new listener
				this.pubsub = new CustomPubSub(channel_function_map);

				// Fire it
				new Thread(() -> connection.subscribe(pubsub, channel)).start();

				// No other way to find out if listener is ready for receiving without polling the listener
				while(!this.pubsub.isSubscribed()){
					try{
						Thread.sleep(2);
					}catch(InterruptedException ignored){
					}
				}
			}
		}finally{
			lock.unlock();
		}
	}

	public void unsubscribe(@Nonnull String channel) {

		// Check input
		if(channel.isEmpty()) throw new IllegalArgumentException("channel is empty");

		// Lock it
		lock.lock();
		try{

			// If pubsub is already empty, ignore
			if(this.pubsub == null) return;

			// Unsubscribe
			this.pubsub.unsubscribe(channel);
			this.channel_function_map.remove(channel);

			// Kill the thread if nothing is subscribed
			if(this.channel_function_map.isEmpty()){
				if(this.pubsub.isSubscribed()) this.pubsub.unsubscribe();
				this.pubsub = null;
			}
		}finally{
			lock.unlock();
		}
	}

	public synchronized void unsubscribeAll(){
		for(String channel : this.channel_function_map.keySet()){
			this.unsubscribe(channel);
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

			// Get consumer function
			Consumer<String> function = channel_function_map.get(channel);

			// If not null, run the function
			if(function != null) function.accept(message);

				// If consumer function is null, then channel name itself is unrecognized by this pubsub
			else{
				System.err.println("Unrecognized channel name '" + channel + "' showed up in PubSub onMessage method");
			}
		}
	}
}
