package com.tomansill.redis.jedis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class JedisPubSubManager{

	private final Jedis connection;

	private final ConcurrentHashMap<String, Consumer<String>> channel_function_map = new ConcurrentHashMap<>();

	private CustomPubSub pubsub = null;

	public JedisPubSubManager(final Jedis jedis){
		this.connection = jedis;
	}

	public synchronized void subscribe(final String channel, final Consumer<String> function){

		// Check input
		if(channel == null) throw new IllegalArgumentException("channel is null");
		if(channel.isEmpty()) throw new IllegalArgumentException("channel is empty");
		if(function == null) throw new IllegalArgumentException("function is null");

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
	}

	public synchronized void unsubscribe(final String channel){

		// Check input
		if(channel == null) throw new IllegalArgumentException("channel is null");
		if(channel.isEmpty()) throw new IllegalArgumentException("channel is empty");

		// Unsubscribe
		this.pubsub.unsubscribe(channel);
		this.channel_function_map.remove(channel);

		// Kill the thread if nothing is subscribed
		if(this.channel_function_map.isEmpty()){
			this.pubsub.unsubscribe();
			this.pubsub = null;
		}
	}

	public synchronized void unsubscribeAll(){
		for(String channel : this.channel_function_map.keySet()){
			this.unsubscribe(channel);
		}
	}

	private class CustomPubSub extends JedisPubSub{
		@Override
		public void onMessage(final String channel, final String message){

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
