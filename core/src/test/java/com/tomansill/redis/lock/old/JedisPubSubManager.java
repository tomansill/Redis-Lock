package com.tomansill.redis.lock.old;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

public class JedisPubSubManager {

	private final Jedis connection;

	private final Map<String, Consumer<String>> channel_function_map = new HashMap<>();

	private CustomPubSub pubsub = null;

	private final ReentrantReadWriteLock rrwl = new ReentrantReadWriteLock();

	public JedisPubSubManager(Jedis jedis){
		this.connection = jedis;
	}

	public void subscribe(String channel, Consumer<String> function){

		// Check input
		if(channel == null) throw new IllegalArgumentException("channel is null");
		if(channel.isEmpty()) throw new IllegalArgumentException("channel is empty");
		if(function == null) throw new IllegalArgumentException("function is null");

		Lock lock = rrwl.writeLock();
		lock.lock();
		try{
			// Add in the map
			this.channel_function_map.put(channel, function);

			// Subscribe if listener already exists
			if(this.pubsub != null) this.pubsub.subscribe(channel);

			else{

				// Create new listener
				this.pubsub = new CustomPubSub();

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

	public void unsubscribe(String channel){
		this.unsubscribe(channel, true);
	}

	private void unsubscribe(String channel, boolean do_lock){

		// Check input
		if(channel == null) throw new IllegalArgumentException("channel is null");
		if(channel.isEmpty()) throw new IllegalArgumentException("channel is empty");

		Lock lock = rrwl.writeLock();
		if(do_lock) lock.lock();
		try{
			// Unsubscribe
			this.pubsub.unsubscribe(channel);
			this.channel_function_map.remove(channel);

			// Kill the thread if nothing is subscribed
			if(this.channel_function_map.isEmpty()){
				if(this.pubsub.isSubscribed()){
					try{
						this.pubsub.unsubscribe();
					}catch(NullPointerException ignored){
						// Apparently jedis unsubscribe is not thread safe?
					}
				}
				this.pubsub = null;
			}
		}finally{
			if(do_lock) lock.unlock();
		}
	}

	public void unsubscribeAll(){
		Lock lock = rrwl.writeLock();
		lock.lock();
		try{
			for(String channel : this.channel_function_map.keySet()) this.unsubscribe(channel, false);
		}finally{
			lock.unlock();
		}
	}

	private class CustomPubSub extends JedisPubSub{
		@Override
		public void onMessage(String channel, String message){

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
