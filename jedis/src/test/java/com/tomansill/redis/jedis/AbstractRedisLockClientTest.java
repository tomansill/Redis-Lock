package com.tomansill.redis.jedis;

import com.tomansill.redis.lock.AbstractRedisLockClient;
import com.tomansill.redis.lock.Utility;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class AbstractRedisLockClientTest{

	private Jedis jedis = new Jedis();
	private AbstractRedisLockClient client = new JedisLockClient(jedis, new Jedis());

	@Test
	public void testSubscribe1() throws InterruptedException{

		// Random channel
		String channel = Utility.generateRandomString(8);
		String message = Utility.generateRandomString(4);

		// CDL and ref
		final CountDownLatch cdl1 = new CountDownLatch(1);
		AtomicReference<String> ref = new AtomicReference<>();

		// Fire it
		client.subscribe(channel, s -> {
			System.out.println("Fired!");
			ref.set(message);
			cdl1.countDown();
		});

		// Assert
		assertNull("Value is not expected.", ref.get());

		// Fire it
		jedis.publish(channel, message);
		System.out.println("published");
		System.out.flush();

		// Await
		cdl1.await(2, TimeUnit.SECONDS);

		// Check result
		assertEquals("Value is not expected.", message, ref.get());

		// Unsubscribe
		client.unsubscribe(channel);
	}

	@Test
	public void testSubscribe2() throws InterruptedException{

		// Random channel
		String channel = Utility.generateRandomString(8);
		String message = Utility.generateRandomString(4);

		// CDL and ref
		final CountDownLatch cdl1 = new CountDownLatch(2);
		AtomicReference<String> ref1 = new AtomicReference<>();
		AtomicReference<String> ref2 = new AtomicReference<>();

		// Fire it
		client.subscribe(channel, s -> {
			System.out.println("Fired!");
			ref1.set(message);
			cdl1.countDown();
		});

		// Fire it
		client.subscribe(channel + "1", s -> {
			System.out.println("Fired!");
			ref2.set(message + "1");
			cdl1.countDown();
		});

		// Assert
		assertNull("Value is not expected.", ref1.get());
		assertNull("Value is not expected.", ref2.get());

		// Fire it
		jedis.publish(channel, message);
		jedis.publish(channel + "1", message);
		System.out.println("published");
		System.out.flush();

		// Await
		cdl1.await(2, TimeUnit.SECONDS);

		// Check result
		assertEquals("Value is not expected.", message, ref1.get());
		assertEquals("Value is not expected.", message + "1", ref2.get());

		// Unsubscribe
		client.unsubscribe(channel + "1");
		client.unsubscribe(channel);
	}

	@Test
	public void testSubscribe3() throws InterruptedException{

		// Random channel
		String channel1 = Utility.generateRandomString(8);
		String message1 = Utility.generateRandomString(4);

		// CDL and ref
		AtomicReference<CountDownLatch> cdl = new AtomicReference<>(new CountDownLatch(1));
		AtomicReference<String> ref = new AtomicReference<>();

		// Fire it
		client.subscribe(channel1, s -> {
			System.out.println("Fired!");
			ref.set(message1);
			cdl.get().countDown();
		});

		// Assert
		assertNull("Value is not expected.", ref.get());

		// Fire it
		jedis.publish(channel1, message1);
		System.out.println("published");
		System.out.flush();

		// Await
		cdl.get().await(2, TimeUnit.SECONDS);

		// Check result
		assertEquals("Value is not expected.", message1, ref.get());

		// Random channel
		String channel2 = Utility.generateRandomString(8);
		String message2 = Utility.generateRandomString(4);

		// CDL and ref
		final CountDownLatch cdl2 = new CountDownLatch(1);

		// Fire it
		client.subscribe(channel2, s -> {
			System.out.println("Fired!");
			ref.set(message2);
			cdl2.countDown();
		});

		// Fire it
		jedis.publish(channel2, message2);
		System.out.println("published");
		System.out.flush();

		// Await
		cdl2.await(2, TimeUnit.SECONDS);

		// Check result
		assertEquals("Value is not expected.", message2, ref.get());

		// Unsubscribe
		client.unsubscribe(channel2);

		// Reset cdl
		cdl.set(new CountDownLatch(1));

		// Fire it
		jedis.publish(channel1, message1);
		System.out.println("published");
		System.out.flush();

		// Await
		cdl.get().await(2, TimeUnit.SECONDS);

		// Check result
		assertEquals("Value is not expected.", message1, ref.get());

		// Unsubscribe
		client.unsubscribe(channel1);
	}
}