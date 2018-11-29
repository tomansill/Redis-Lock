package com.tomansill.redis.lock;

import com.tomansill.redis.jedis.JedisPubSubManager;
import com.tomansill.redis.test.util.ResetableCountDownLatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TestScripts{

	private static final String HOSTNAME = "localhost";

	private static final int PORT = 6379;

	private static final HashMap<String, String> SCRIPT_NAME_TO_SCRIPTS = new HashMap<>();

	private static final String[] FILENAMES = {"single_instance_lock", "single_instance_unlock", "single_instance_refire"};

	private static JedisPool pool = null;

	private static final long VERY_LONG_TIMEOUT = 60000;

	private static final long MARGIN = 40;

	private static final BlockingQueue<String> PUBSUB = new LinkedBlockingQueue<>();

	private static final ResetableCountDownLatch cdl = new ResetableCountDownLatch(1);

	private static JedisPubSubManager pubsub;

	@Before
	public void setUp(){

		// Class loader
		ClassLoader class_loader = getClass().getClassLoader();

		// Load scripts
		for(String filename : FILENAMES){

			// Get file
			File file = new File(class_loader.getResource(filename + ".lua").getFile());

			// Serious error occurs if those files cannot be found
			if(!file.exists()){
				throw new ExceptionInInitializerError("Cannot find '" + filename + ".lua' on the package resouces directory!");
			}

			// Read the script
			StringBuilder sb = new StringBuilder();
			try(BufferedReader br = new BufferedReader(new FileReader(file))){
				String line;
				while((line = br.readLine()) != null){

					sb.append(line);
					sb.append('\n');
				}
			}catch(IOException ioe){
				throw new ExceptionInInitializerError("Failed to read '" + filename + ".lua' on the package resouces directory! Reason: " + ioe.getMessage());
			}

			// Add it to the script
			SCRIPT_NAME_TO_SCRIPTS.put(filename, sb.toString());
		}

		// Create pool
		pool = new JedisPool(new JedisPoolConfig(), HOSTNAME, PORT);

		// Set up pubsub
		Jedis pubsub_listener = pool.getResource();
		pubsub = new JedisPubSubManager(pubsub_listener);
		/*
		pubsub.subscribe("lockchannel", (message -> {
			PUBSUB.add(message);
			cdl.countDown();
		}));
		*/
	}

	@After
	public void tearDown(){
		if(pubsub != null){
			pubsub.close();
			pubsub = null;
		}
		/*
		if(pubsub != null){
			pubsub.unsubscribeAll();
			pubsub = null;
		}
		if(pubsub_listener != null){
			pubsub_listener.close();
			pubsub_listener = null;
		}*/
		if(pool != null){
			pool.close();
			pool = null;
		}
	}

	@Test
	// Maven Junit will attempt to fork tests but it will be messed up in multiple threads, enforce single threading
	public void test() throws InterruptedException{
		this.testSingleInstanceLockScriptUnfairWriteLock();
		this.testSingleInstanceLockScriptFairWriteLock();
		this.testSingleInstanceLockScriptFairReadLock();
		this.testSingleInstanceLockScriptUnfairReadLock();
		this.testSingleInstanceUnlockScript();
	}

	private void testSingleInstanceLockScriptUnfairWriteLock() throws InterruptedException{

		// Check if script is available
		assertTrue("Script name 'single_instance_lock' is not available. We cannot test this", SCRIPT_NAME_TO_SCRIPTS.containsKey("single_instance_lock"));

		// Create a random key
		String key = Utility.generateRandomString(8);

		// Subscribe
		pubsub.subscribe("lockchannel:" + key, (message -> {
			PUBSUB.add(message);
			cdl.countDown();
		}));

		// Grab a connection
		try(Jedis jedis = pool.getResource()){

			try{
				// Load the script
				String script_hash = jedis.scriptLoad(SCRIPT_NAME_TO_SCRIPTS.get("single_instance_lock"));

				// Do a simple successful unfair lock that should always succeed
				cdl.reset(1);
				Object ret_val = jedis.evalsha(script_hash, 10, key, "client1", "one", "0", "0", VERY_LONG_TIMEOUT + "", VERY_LONG_TIMEOUT + "", "0", "", "0");
				assertTrue("Return value is not an integer! value: " + ret_val, ret_val instanceof Long);
				assertEquals("Return value is not zero!", 0, ((Long) ret_val).longValue());
				assertTrue("CDL timed out", cdl.await(10, TimeUnit.MILLISECONDS));
				assertEquals("Pubsub seems messed up... Value: " + PUBSUB, 1, PUBSUB.size());
				assertEquals("Value from Pubsub is messed up", "l:client1:one:" + VERY_LONG_TIMEOUT + ":" + key, PUBSUB.poll());

				// Check expiration date
				long ttl = jedis.pttl("lockpoint:" + key);
				assertFalse("TTL is not what expected. Actual: " + ttl, (ttl == -1) || (ttl == -2) || (ttl > VERY_LONG_TIMEOUT));

				// Check value
				assertEquals("Lockpoint value is not what expected", "unique", jedis.get("lockpoint:" + key));

				// Check pubsub
				assertEquals("PUBSUB is not empty! Value:" + PUBSUB, 0, PUBSUB.size());

				// Check values that should not exist
				assertFalse("lockwait should not exist", jedis.exists("lockwait:" + key));
				assertFalse("lockpool should not exist", jedis.exists("lockpool:" + key));
				assertFalse("lockcount should not exist", jedis.exists("lockcount:" + key));

				// Do unfair lock that should fail
				ret_val = jedis.evalsha(script_hash, 10, key, "client1", "two", "0", "1", VERY_LONG_TIMEOUT + "", (VERY_LONG_TIMEOUT * 2) + "", "0", "", "0");
				assertTrue("Return value is not an integer!", ret_val instanceof Long);
				assertEquals("Return value does not match!", VERY_LONG_TIMEOUT - MARGIN, (Long) ret_val, MARGIN);
				assertTrue("Return value is not positive integer! Value:" + ret_val, 0 < (Long) ret_val);

				// Check stuff that should not exist
				assertFalse("lockwait should not exist", jedis.exists("lockwait:" + key));
				assertFalse("lockpool should not exist", jedis.exists("lockpool:" + key));
				assertFalse("lockcount should not exist", jedis.exists("lockcount:" + key));

				// Check pubsub
				assertEquals("PUBSUB is not empty! Value:" + PUBSUB, 0, PUBSUB.size());
			}finally{
				jedis.del("lockpoint:" + key);
				PUBSUB.clear();
				pubsub.unsubscribe("lockchannel:" + key);
			}
		}
	}

	private void testSingleInstanceLockScriptFairWriteLock() throws InterruptedException{

		// Check if script is available
		assertTrue("Script name 'single_instance_lock' is not available. We cannot test this", SCRIPT_NAME_TO_SCRIPTS.containsKey("single_instance_lock"));

		// Create a random key
		String key = Utility.generateRandomString(8);

		// Subscribe
		pubsub.subscribe("lockchannel:" + key, (message -> {
			PUBSUB.add(message);
			cdl.countDown();
		}));

		// Grab a connection
		try(Jedis jedis = pool.getResource()){

			try{
				// Load the script
				String script_hash = jedis.scriptLoad(SCRIPT_NAME_TO_SCRIPTS.get("single_instance_lock"));

				// Do a simple successful fair lock that should always succeed
				cdl.reset(1);
				Object ret_val = jedis.evalsha(script_hash, 10, key, "client1", "one", "1", "1", VERY_LONG_TIMEOUT + "", (VERY_LONG_TIMEOUT * 2) + "", "0", "", "0");
				assertTrue("Return value is not an integer!", ret_val instanceof Long);
				assertEquals("Return value is not zero!", ((Long) ret_val).longValue(), 0);
				assertTrue("CDL timed out", cdl.await(10, TimeUnit.MILLISECONDS));
				assertEquals("Pubsub seems messed up... Value: " + PUBSUB, 1, PUBSUB.size());
				assertEquals("Value from Pubsub is messed up", "l:client1:one:" + VERY_LONG_TIMEOUT + ":" + key, PUBSUB.poll());

				// Check lockpoint expiration date
				assertEquals("TTL on lockpoint is not what expected", VERY_LONG_TIMEOUT - MARGIN, jedis.pttl("lockpoint:" + key), MARGIN);

				// Check value
				assertEquals("Lockpoint value is not what expected", "unique", jedis.get("lockpoint:" + key));

				// Do another fair lock that should fail
				ret_val = jedis.evalsha(script_hash, 10, key, "client1", "two", "1", "1", VERY_LONG_TIMEOUT + "", (VERY_LONG_TIMEOUT * 2) + "", "0", "", "0");
				assertTrue("Return value is not an integer!", ret_val instanceof Long);
				assertEquals("Return value does not match!", VERY_LONG_TIMEOUT - MARGIN, (Long) ret_val, MARGIN);
				assertTrue("Return value is not positive integer! Value:" + ret_val, 0 < (Long) ret_val);

				// Check lockwait
				assertTrue("Lockwait doesn't exist", jedis.exists("lockwait:" + key));
				List<String> lockwait = jedis.lrange("lockwait:" + key, 0, -1);
				assertEquals("TTL on lockwait is not what expected", (VERY_LONG_TIMEOUT * 2) - MARGIN, jedis.pttl("lockwait:" + key), MARGIN);
				assertEquals("Lockwait either contains more than one element or is empty", 1, lockwait.size());
				assertEquals("Lockwait element doesn't contain the expected element", "client1:two", lockwait.get(0));

				// Do another one
				ret_val = jedis.evalsha(script_hash, 10, key, "client1", "three", "1", "1", VERY_LONG_TIMEOUT + "", (VERY_LONG_TIMEOUT * 2) + "", "0", "", "0");
				if(ret_val instanceof String){
					ret_val = new Long((String) ret_val);
				}
				assertTrue(
						"Return value is not an integer! value: " + (ret_val == null ? "null" : ret_val + " " + ret_val.getClass().getName()),
						ret_val instanceof Long
				);
				assertEquals("Return value does not match!", VERY_LONG_TIMEOUT - 20, (Long) ret_val, 20);
				assertTrue("Return value is not positive integer! Value:" + ret_val, 0 < (Long) ret_val);

				// Check lockwait
				assertTrue("Lockwait doesn't exist", jedis.exists("lockwait:" + key));
				lockwait = jedis.lrange("lockwait:" + key, 0, -1);
				assertEquals("TTL on lockwait is not what expected", (VERY_LONG_TIMEOUT * 2) - 20, jedis.pttl("lockwait:" + key), 20);
				assertEquals("Lockwait either does not consist of expected two elements", 2, lockwait.size());
				assertEquals("Lockwait element doesn't contain the expected elements", "client1:two", lockwait.get(0));
				assertEquals("Lockwait element doesn't contain the expected elements", "client1:three", lockwait.get(1));

				// Check pubsub
				assertEquals("PUBSUB is not empty! Value:" + PUBSUB, 0, PUBSUB.size());

				// Do unfair lock that should fail
				ret_val = jedis.evalsha(script_hash, 10, key, "client1", "four", "0", "1", VERY_LONG_TIMEOUT + "", (VERY_LONG_TIMEOUT * 2) + "", "0", "", "0");
				assertTrue("Return value is not an integer!", ret_val instanceof Long);
				assertEquals("Return value does not match!", VERY_LONG_TIMEOUT - MARGIN, (Long) ret_val, MARGIN);
				assertTrue("Return value is not positive integer! Value:" + ret_val, 0 < (Long) ret_val);

				// Check lockwait
				assertTrue("Lockwait doesn't exist", jedis.exists("lockwait:" + key));
				lockwait = jedis.lrange("lockwait:" + key, 0, -1);
				assertEquals("TTL on lockwait is not what expected", (VERY_LONG_TIMEOUT * 2) - 20, jedis.pttl("lockwait:" + key), 20);
				assertEquals("Lockwait either does not consist of expected two elements", 2, lockwait.size());

				// Remove the lock
				jedis.del("lockpoint:" + key);

				// Do unfair lock that should succeed (unfairly take the point)
				cdl.reset(1);
				ret_val = jedis.evalsha(script_hash, 10, key, "client1", "five", "0", "1", VERY_LONG_TIMEOUT + "", (VERY_LONG_TIMEOUT * 2) + "", "0", "", "0");
				assertTrue("Return value is not an integer!", ret_val instanceof Long);
				assertEquals("Return value is not zero!", ((Long) ret_val).longValue(), 0);
				assertTrue("CDL timed out", cdl.await(10, TimeUnit.MILLISECONDS));
				assertEquals("Pubsub seems messed up... Value: " + PUBSUB, 1, PUBSUB.size());
				assertEquals("Value from Pubsub is messed up", "l:client1:five:" + VERY_LONG_TIMEOUT + ":" + key, PUBSUB.poll());

				// Check lockwait (should be unchanged)
				assertTrue("Lockwait doesn't exist", jedis.exists("lockwait:" + key));
				lockwait = jedis.lrange("lockwait:" + key, 0, -1);
				assertEquals("TTL on lockwait is not what expected", (VERY_LONG_TIMEOUT * 2) - 20, jedis.pttl("lockwait:" + key), 20);
				assertEquals("Lockwait either does not consist of expected two elements", 2, lockwait.size());

				// Remove the lock
				jedis.del("lockpoint:" + key);

				// Do another fair lock that should fail (it should jump straight to queue)
				ret_val = jedis.evalsha(script_hash, 10, key, "client1", "six", "1", "1", VERY_LONG_TIMEOUT + "", (VERY_LONG_TIMEOUT * 2) + "", "0", "", "0");
				assertTrue("Return value is not an integer!", ret_val instanceof Long);
				assertEquals("Return value does not match!", -2, ((Long) ret_val).longValue());
				//assertEquals("Return value does not match!", VERY_LONG_TIMEOUT - MARGIN, (Long) ret_val, MARGIN);
				//assertTrue("Return value is not positive integer! Value:" + ret_val, 0 < (Long) ret_val);

				// Check lockwait
				assertTrue("Lockwait doesn't exist", jedis.exists("lockwait:" + key));
				lockwait = jedis.lrange("lockwait:" + key, 0, -1);
				assertEquals("TTL on lockwait is not what expected", (VERY_LONG_TIMEOUT * 2) - 20, jedis.pttl("lockwait:" + key), 20);
				assertEquals("Lockwait either contains more than one element or is empty", 3, lockwait.size());
				assertEquals("Lockwait element doesn't contain the expected element", "client1:two", lockwait.get(0));
				assertEquals("Lockwait element doesn't contain the expected elements", "client1:three", lockwait.get(1));
				assertEquals("Lockwait element doesn't contain the expected elements", "client1:six", lockwait.get(2));

				// Check pubsub
				assertEquals("PUBSUB is not empty! Value:" + PUBSUB, 0, PUBSUB.size());
			}finally{
				jedis.del("lockpoint:" + key);
				jedis.del("lockwait:" + key);
				PUBSUB.clear();
				pubsub.unsubscribe("lockchannel:" + key);
			}
		}
	}

	private void testSingleInstanceLockScriptUnfairReadLock() throws InterruptedException{

		// Check if script is available
		assertTrue("Script name 'single_instance_lock' is not available. We cannot test this", SCRIPT_NAME_TO_SCRIPTS.containsKey("single_instance_lock"));

		// Create a random key
		String key = Utility.generateRandomString(8);

		// Subscribe
		pubsub.subscribe("lockchannel:" + key, (message -> {
			PUBSUB.add(message);
			cdl.countDown();
		}));

		// Grab a connection
		try(Jedis jedis = pool.getResource()){
			try{

				// Load the script
				String script_hash = jedis.scriptLoad(SCRIPT_NAME_TO_SCRIPTS.get("single_instance_lock"));

				// Do a simple successful fair lock that should always succeed
				cdl.reset(1);
				Object ret_val = jedis.evalsha(script_hash, 10, key, "client1", "one", "0", "1", VERY_LONG_TIMEOUT + "", (VERY_LONG_TIMEOUT * 2) + "", "1", "", "0");
				assertTrue("Return value is not an integer!", ret_val instanceof Long);
				assertEquals("Return value is not zero!", ((Long) ret_val).longValue(), 0);
				assertTrue("CDL timed out", cdl.await(10, TimeUnit.MILLISECONDS));
				assertEquals("Value from Pubsub is messed up", "o:" + key, PUBSUB.poll()); // TODO was 's' now o
				assertEquals("Value from Pubsub is messed up", "l:client1:one:" + VERY_LONG_TIMEOUT + ":" + key, PUBSUB.poll(MARGIN, TimeUnit.MILLISECONDS));

				// Check lockpoint expiration date
				assertEquals("TTL on lockpoint is not what expected", VERY_LONG_TIMEOUT - MARGIN, jedis.pttl("lockpoint:" + key), MARGIN);

				// Check value
				assertEquals("Lockpoint value is not what expected", "open", jedis.get("lockpoint:" + key));
				assertEquals("Lockcount value is not what expected", "1", jedis.get("lockcount:" + key));
				assertFalse("lockwait should not exist", jedis.exists("lockwait:" + key));
				assertFalse("lockpool should not exist", jedis.exists("lockpool:" + key));

				// Do unfair read lock - it should succeed
				cdl.reset(1);
				ret_val = jedis.evalsha(script_hash, 10, key, "client1", "two", "0", "1", VERY_LONG_TIMEOUT + "", (VERY_LONG_TIMEOUT * 2) + "", "1", "", "0");
				assertTrue("Return value is not an integer!", ret_val instanceof Long);
				assertEquals("Return value is not zero!", ((Long) ret_val).longValue(), -3); // TODO was 0, now -3 because shared lock success
				assertTrue("CDL timed out", cdl.await(10, TimeUnit.MILLISECONDS));
				assertEquals("Pubsub seems messed up... Value: " + PUBSUB, 1, PUBSUB.size());
				assertEquals("Value from Pubsub is messed up", "l:client1:two:" + VERY_LONG_TIMEOUT + ":" + key, PUBSUB.poll());

				// Check lockcount
				assertEquals("Lockcount value is not what expected", "2", jedis.get("lockcount:" + key));
				assertFalse("lockwait should not exist", jedis.exists("lockwait:" + key));
				assertFalse("lockpool should not exist", jedis.exists("lockpool:" + key));

				// Do unfair write lock that should fail
				ret_val = jedis.evalsha(script_hash, 10, key, "client1", "four", "0", "1", VERY_LONG_TIMEOUT + "", (VERY_LONG_TIMEOUT * 2) + "", "0", "", "0");
				assertTrue("Return value is not an integer!", ret_val instanceof Long);
				assertEquals("Return value does not match!", VERY_LONG_TIMEOUT - MARGIN, (Long) ret_val, MARGIN);
				assertTrue("Return value is not positive integer! Value:" + ret_val, 0 < (Long) ret_val);

				// Check lockcount
				assertEquals("Lockcount value is not what expected", "2", jedis.get("lockcount:" + key));
				assertFalse("lockwait should not exist", jedis.exists("lockwait:" + key));
				assertFalse("lockpool should not exist", jedis.exists("lockpool:" + key));

				// Do fair write lock that should fail
				ret_val = jedis.evalsha(script_hash, 10, key, "client1", "four", "1", "1", VERY_LONG_TIMEOUT + "", (VERY_LONG_TIMEOUT * 2) + "", "0", "", "0");
				assertTrue("Return value is not an integer!", ret_val instanceof Long);
				assertEquals("Return value does not match!", VERY_LONG_TIMEOUT - MARGIN, (Long) ret_val, MARGIN);
				assertTrue("Return value is not positive integer! Value:" + ret_val, 0 < (Long) ret_val);

				// Check lockcount
				assertEquals("Lockcount value is not what expected", "2", jedis.get("lockcount:" + key));
				assertTrue("lockwait should exist", jedis.exists("lockwait:" + key));
				assertFalse("lockpool should not exist", jedis.exists("lockpool:" + key));

				// Manually change lockpoint to closed and check value
				jedis.set("lockpoint:" + key, "closed");
				jedis.del("lockwait:" + key);

				// Do unfair readlock - it should succeed
				cdl.reset(1);
				ret_val = jedis.evalsha(script_hash, 10, key, "client1", "three", "0", "1", VERY_LONG_TIMEOUT + "", (VERY_LONG_TIMEOUT * 2) + "", "1", "", "0");
				assertTrue("Return value is not an integer!", ret_val instanceof Long);
				assertEquals("Return value is not zero!", ((Long) ret_val).longValue(), -3); // TODO was 0, now -3 because shared lock success
				assertTrue("CDL timed out", cdl.await(10, TimeUnit.MILLISECONDS));
				assertEquals("Pubsub seems messed up... Value: " + PUBSUB, 1, PUBSUB.size());
				assertEquals("Value from Pubsub is messed up", "l:client1:three:" + VERY_LONG_TIMEOUT + ":" + key, PUBSUB.poll());

				// Check lockcount
				assertEquals("Lockcount value is not what expected", "3", jedis.get("lockcount:" + key));
				assertFalse("lockwait should not exist", jedis.exists("lockwait:" + key));
				assertFalse("lockpool should not exist", jedis.exists("lockpool:" + key));

				// Do unfair write lock that should fail
				ret_val = jedis.evalsha(script_hash, 10, key, "client1", "four", "0", "1", VERY_LONG_TIMEOUT + "", (VERY_LONG_TIMEOUT * 2) + "", "0", "", "0");
				assertTrue("Return value is not an integer!", ret_val instanceof Long);
				assertEquals("Return value does not match!", VERY_LONG_TIMEOUT - MARGIN, (Long) ret_val, MARGIN);
				assertTrue("Return value is not positive integer! Value:" + ret_val, 0 < (Long) ret_val);

				// Check lockcount
				assertEquals("Lockcount value is not what expected", "3", jedis.get("lockcount:" + key));
				assertFalse("lockwait should not exist", jedis.exists("lockwait:" + key));
				assertFalse("lockpool should not exist", jedis.exists("lockpool:" + key));

				// Do fair write lock that should fail
				ret_val = jedis.evalsha(script_hash, 10, key, "client1", "four", "1", "1", VERY_LONG_TIMEOUT + "", (VERY_LONG_TIMEOUT * 2) + "", "0", "", "0");
				assertTrue("Return value is not an integer!", ret_val instanceof Long);
				assertEquals("Return value does not match!", VERY_LONG_TIMEOUT - MARGIN, (Long) ret_val, MARGIN);
				assertTrue("Return value is not positive integer! Value:" + ret_val, 0 < (Long) ret_val);

				// Check lockcount
				assertEquals("Lockcount value is not what expected", "3", jedis.get("lockcount:" + key));
				assertTrue("lockwait should exist", jedis.exists("lockwait:" + key));
				assertFalse("lockpool should not exist", jedis.exists("lockpool:" + key));

			}finally{
				jedis.del("lockpoint:" + key);
				jedis.del("lockwait:" + key);
				jedis.del("lockpool:" + key);
				jedis.del("lockcount:" + key);
				PUBSUB.clear();
				pubsub.unsubscribe("lockchannel:" + key);
			}
		}
	}

	private void testSingleInstanceLockScriptFairReadLock() throws InterruptedException{

		// Check if script is available
		assertTrue("Script name 'single_instance_lock' is not available. We cannot test this", SCRIPT_NAME_TO_SCRIPTS.containsKey("single_instance_lock"));

		// Create a random key
		String key = Utility.generateRandomString(8);

		// Subscribe
		pubsub.subscribe("lockchannel:" + key, (message -> {
			PUBSUB.add(message);
			cdl.countDown();
		}));

		// Grab a connection
		try(Jedis jedis = pool.getResource()){
			try{
				// Load the script
				String script_hash = jedis.scriptLoad(SCRIPT_NAME_TO_SCRIPTS.get("single_instance_lock"));

				// Do a simple successful fair lock that should always succeed
				cdl.reset(1);
				Object ret_val = jedis.evalsha(script_hash, 10, key, "client1", "one", "1", "0", VERY_LONG_TIMEOUT + "", VERY_LONG_TIMEOUT + "", "1", "", "0");
				assertTrue("Return value is not an integer! value: " + ret_val, ret_val instanceof Long);
				assertEquals("Return value is not zero!", 0, ((Long) ret_val).longValue());
				assertTrue("CDL timed out", cdl.await(10, TimeUnit.MILLISECONDS));
				assertEquals("Value from Pubsub is messed up", "o:" + key, PUBSUB.poll()); // TODO was 's' now 'o'
				assertEquals("Value from Pubsub is messed up", "l:client1:one:" + VERY_LONG_TIMEOUT + ":" + key, PUBSUB.poll(MARGIN, TimeUnit.MILLISECONDS));

				// Check lockpoint expiration date
				assertEquals("TTL on lockpoint is not what expected", VERY_LONG_TIMEOUT - MARGIN, jedis.pttl("lockpoint:" + key), MARGIN);

				// Check value
				assertEquals("Lockpoint value is not what expected", "open", jedis.get("lockpoint:" + key));
				assertEquals("Lockcount value is not what expected", "1", jedis.get("lockcount:" + key));

				// Check values that shouldnt exist
				assertFalse("lockwait should not exist", jedis.exists("lockwait:" + key));
				assertFalse("lockpool should not exist", jedis.exists("lockpool:" + key));

				// Do an unfair write lock that should fail
				ret_val = jedis.evalsha(script_hash, 10, key, "client1", "two", "0", "1", VERY_LONG_TIMEOUT + "", (VERY_LONG_TIMEOUT * 2) + "", "0", "", "0");
				assertTrue("Return value is not an integer!", ret_val instanceof Long);
				assertEquals("Return value does not match!", VERY_LONG_TIMEOUT - MARGIN, (Long) ret_val, MARGIN);
				assertTrue("Return value is not positive integer! Value:" + ret_val, 0 < (Long) ret_val);

				// Check lockcount
				assertEquals("Lockcount value is not what expected", "1", jedis.get("lockcount:" + key));
				assertFalse("lockwait should not exist", jedis.exists("lockwait:" + key));
				assertFalse("lockpool should not exist", jedis.exists("lockpool:" + key));

				// Do a fair write lock that should fail
				ret_val = jedis.evalsha(script_hash, 10, key, "client1", "three", "1", "1", VERY_LONG_TIMEOUT + "", (VERY_LONG_TIMEOUT * 2) + "", "0", "", "0");
				assertTrue("Return value is not an integer!", ret_val instanceof Long);
				assertEquals("Return value does not match!", VERY_LONG_TIMEOUT - MARGIN, (Long) ret_val, MARGIN);
				assertTrue("Return value is not positive integer! Value:" + ret_val, 0 < (Long) ret_val);

				// Check lockcount
				assertEquals("Lockcount value is not what expected", "1", jedis.get("lockcount:" + key));
				assertTrue("lockwait should exist", jedis.exists("lockwait:" + key));
				assertFalse("lockpool should not exist", jedis.exists("lockpool:" + key));

				// Clean up
				jedis.del("lockwait:" + key);

				// Do fair readlock - it should succeed
				cdl.reset(1);
				ret_val = jedis.evalsha(script_hash, 10, key, "client1", "four", "1", "1", VERY_LONG_TIMEOUT + "", (VERY_LONG_TIMEOUT * 2) + "", "1", "", "0");
				assertTrue("Return value is not an integer!", ret_val instanceof Long);
				assertEquals("Return value is not zero!", ((Long) ret_val).longValue(), -3); // TODO was 0, now -3 because shared lock success
				assertTrue("CDL timed out", cdl.await(10, TimeUnit.MILLISECONDS));
				assertEquals("Pubsub seems messed up... Value: " + PUBSUB, 1, PUBSUB.size());
				assertEquals("Value from Pubsub is messed up", "l:client1:four:" + VERY_LONG_TIMEOUT + ":" + key, PUBSUB.poll());

				assertEquals("Lockcount value is not what expected", "2", jedis.get("lockcount:" + key));
				assertFalse("lockwait should not exist", jedis.exists("lockwait:" + key));
				assertFalse("lockpool should not exist", jedis.exists("lockpool:" + key));

				// Do unfair readlock - it should succeed
				cdl.reset(1);
				ret_val = jedis.evalsha(script_hash, 10, key, "client1", "five", "0", "1", VERY_LONG_TIMEOUT + "", (VERY_LONG_TIMEOUT * 2) + "", "1", "", "0");
				assertTrue("Return value is not an integer!", ret_val instanceof Long);
				assertEquals("Return value is not zero!", ((Long) ret_val).longValue(), -3); // TODO was 0, now -3 because shared lock success
				assertTrue("CDL timed out", cdl.await(10, TimeUnit.MILLISECONDS));
				assertEquals("Pubsub seems messed up... Value: " + PUBSUB, 1, PUBSUB.size());
				assertEquals("Value from Pubsub is messed up", "l:client1:five:" + VERY_LONG_TIMEOUT + ":" + key, PUBSUB.poll());

				assertEquals("Lockcount value is not what expected", "3", jedis.get("lockcount:" + key));
				assertFalse("lockwait should not exist", jedis.exists("lockwait:" + key));
				assertFalse("lockpool should not exist", jedis.exists("lockpool:" + key));

				// Manually close the lock
				jedis.set("lockpoint:" + key, "closed");
				jedis.pexpire("lockpoint:" + key, VERY_LONG_TIMEOUT); // TODO address this later?

				// Do an unfair write lock that should fail
				ret_val = jedis.evalsha(script_hash, 10, key, "client1", "two", "0", "1", VERY_LONG_TIMEOUT + "", (VERY_LONG_TIMEOUT * 2) + "", "0", "", "0");
				assertTrue("Return value is not an integer!", ret_val instanceof Long);
				assertEquals("Return value does not match!", VERY_LONG_TIMEOUT - MARGIN, (Long) ret_val, MARGIN);
				assertTrue("Return value is not positive integer! Value:" + ret_val, 0 < (Long) ret_val);

				// Check lockcount
				assertEquals("Lockcount value is not what expected", "3", jedis.get("lockcount:" + key));
				assertFalse("lockwait should not exist", jedis.exists("lockwait:" + key));
				assertFalse("lockpool should not exist", jedis.exists("lockpool:" + key));

				// Do a fair write lock that should fail
				ret_val = jedis.evalsha(script_hash, 10, key, "client1", "three", "1", "1", VERY_LONG_TIMEOUT + "", (VERY_LONG_TIMEOUT * 2) + "", "0", "", "0");
				assertTrue("Return value is not an integer!", ret_val instanceof Long);
				assertEquals("Return value does not match!", VERY_LONG_TIMEOUT - MARGIN, (Long) ret_val, MARGIN);
				assertTrue("Return value is not positive integer! Value:" + ret_val, 0 < (Long) ret_val);

				// Check lockcount
				assertEquals("Lockcount value is not what expected", "3", jedis.get("lockcount:" + key));
				assertTrue("lockwait should exist", jedis.exists("lockwait:" + key));
				assertFalse("lockpool should not exist", jedis.exists("lockpool:" + key));
				assertFalse("lockwait should not contain 'S'", new HashSet<>(jedis.lrange("lockwait:" + key, 0, -1)).contains("S"));

				// Clean up
				jedis.del("lockwait:" + key);

				// Do fair readlock - it should fail
				ret_val = jedis.evalsha(script_hash, 10, key, "client1", "six", "1", "1", VERY_LONG_TIMEOUT + "", (VERY_LONG_TIMEOUT * 2) + "", "1", "", "0");
				assertTrue("Return value is not an integer!", ret_val instanceof Long);
				assertEquals("Return value does not match!", VERY_LONG_TIMEOUT - MARGIN, (Long) ret_val, MARGIN);

				// Check lockcount
				assertEquals("Lockcount value is not what expected", "3", jedis.get("lockcount:" + key));
				assertTrue("lockwait should exist", jedis.exists("lockwait:" + key));
				assertTrue("lockpool should exist", jedis.exists("lockpool:" + key));
				assertTrue("lockwait should contain 'S'", new HashSet<>(jedis.lrange("lockwait:" + key, 0, -1)).contains("S"));

				// Do unfair readlock - it should succeed
				cdl.reset(1);
				ret_val = jedis.evalsha(script_hash, 10, key, "client1", "seven", "0", "1", VERY_LONG_TIMEOUT + "", (VERY_LONG_TIMEOUT * 2) + "", "1", "", "0");
				assertTrue("Return value is not an integer!", ret_val instanceof Long);
				assertEquals("Return value is not zero!", ((Long) ret_val).longValue(), -3);  // TODO was 0, now -3 because shared lock success
				assertTrue("CDL timed out", cdl.await(10, TimeUnit.MILLISECONDS));
				assertEquals("Pubsub seems messed up... Value: " + PUBSUB, 1, PUBSUB.size());
				assertEquals("Value from Pubsub is messed up", "l:client1:seven:" + VERY_LONG_TIMEOUT + ":" + key, PUBSUB.poll());

				assertEquals("Lockcount value is not what expected", "4", jedis.get("lockcount:" + key));
				assertTrue("lockwait should exist", jedis.exists("lockwait:" + key));
				assertTrue("lockpool should exist", jedis.exists("lockpool:" + key));

			}finally{
				jedis.del("lockpoint:" + key);
				jedis.del("lockwait:" + key);
				jedis.del("lockpool:" + key);
				jedis.del("lockcount:" + key);
				PUBSUB.clear();
				pubsub.unsubscribe("lockchannel:" + key);
			}
		}
	}

	private void testSingleInstanceUnlockScript() throws InterruptedException{

		// Check if script is available
		assertTrue("Script name 'single_instance_unlock' is not available. We cannot test this", SCRIPT_NAME_TO_SCRIPTS.containsKey("single_instance_unlock"));

		// Create a random key
		String key = Utility.generateRandomString(8);

		// Subscribe
		pubsub.subscribe("lockchannel:" + key, (message -> {
			PUBSUB.add(message);
			cdl.countDown();
		}));

		// Grab a connection
		try(Jedis jedis = pool.getResource()){
			try{
				// Load the script
				String script_hash = jedis.scriptLoad(SCRIPT_NAME_TO_SCRIPTS.get("single_instance_unlock"));

				// Set up simple writelock with nothing in queue
				jedis.set("lockpoint:" + key, "unique");
				jedis.pexpire("lockpoint:" + key, VERY_LONG_TIMEOUT);

				// Run unlock
				jedis.evalsha(script_hash, 4, key, "0", "", "0");
				assertEquals("Value from Pubsub is messed up", "#:" + key, PUBSUB.poll(MARGIN, TimeUnit.MILLISECONDS));
				assertFalse("Lockpoint shouldnt exist", jedis.exists("lockpoint:" + key));

				// Set up simple writelock with something in queue
				jedis.set("lockpoint:" + key, "unique");
				jedis.pexpire("lockpoint:" + key, VERY_LONG_TIMEOUT);
				jedis.lpush("lockwait:" + key, "client1:one");
				jedis.pexpire("lockwait:" + key, VERY_LONG_TIMEOUT);

				// Run unlock
				jedis.evalsha(script_hash, 4, key, "0", "", "0");
				assertEquals("Value from Pubsub is messed up", "o:client1:one:" + key, PUBSUB.poll(MARGIN, TimeUnit.MILLISECONDS));
				assertFalse("Lockpoint shouldnt exist", jedis.exists("lockpoint:" + key));

				// Set up simple readlock with no other readlocks
				jedis.set("lockpoint:" + key, "unique");
				jedis.pexpire("lockpoint:" + key, VERY_LONG_TIMEOUT);
				jedis.set("lockcount:" + key, "1");
				jedis.pexpire("lockcount:" + key, VERY_LONG_TIMEOUT);
				jedis.lset("lockwait:" + key, 0, "client1:two");

				// Run unlock
				jedis.evalsha(script_hash, 4, key, "1", "", "0");
				assertEquals("Value from Pubsub is messed up", "o:client1:two:" + key, PUBSUB.poll(MARGIN, TimeUnit.MILLISECONDS));
				assertFalse("Lockpoint shouldnt exist", jedis.exists("lockpoint:" + key));
				assertFalse("Lockcount shouldnt exist", jedis.exists("lockcount:" + key));

				// Set up simple readlock with other readlocks
				jedis.set("lockpoint:" + key, "open");
				jedis.pexpire("lockpoint:" + key, VERY_LONG_TIMEOUT);
				jedis.set("lockcount:" + key, "3");
				jedis.pexpire("lockcount:" + key, VERY_LONG_TIMEOUT);

				// Run unlock
				jedis.evalsha(script_hash, 4, key, "1", "", "0");
				assertTrue("Lockpoint should exist", jedis.exists("lockpoint:" + key));
				assertTrue("Lockcount should exist", jedis.exists("lockcount:" + key));
				assertEquals("Lockpoint is wrong", "open", jedis.get("lockpoint:" + key));
				assertEquals("Lockcount is wrong", "2", jedis.get("lockcount:" + key));

				// Run unlock with owner mode
				jedis.evalsha(script_hash, 4, key, "1", "", "1");
				assertTrue("Lockpoint should exist", jedis.exists("lockpoint:" + key));
				assertTrue("Lockcount should exist", jedis.exists("lockcount:" + key));
				assertEquals("Lockpoint is wrong", "closed", jedis.get("lockpoint:" + key));
				assertEquals("Lockcount is wrong", "1", jedis.get("lockcount:" + key));

				// Run unlock one more time
				jedis.del("lockwait:" + key);
				jedis.evalsha(script_hash, 4, key, "1", "", "0");
				assertEquals("Value from Pubsub is messed up", "#:" + key, PUBSUB.poll(MARGIN, TimeUnit.MILLISECONDS));
				assertFalse("Lockpoint shouldnt exist", jedis.exists("lockpoint:" + key));
				assertFalse("Lockcount shouldnt exist", jedis.exists("lockcount:" + key));

			}finally{
				jedis.del("lockpoint:" + key);
				jedis.del("lockwait:" + key);
				jedis.del("lockpool:" + key);
				jedis.del("lockcount:" + key);
				PUBSUB.clear();
				pubsub.unsubscribe("lockchannel:" + key);
			}
		}
	}

	private void testSingleInstanceRefirecript() throws InterruptedException{

		// Check if script is available
		assertTrue("Script name 'single_instance_refire' is not available. We cannot test this", SCRIPT_NAME_TO_SCRIPTS.containsKey("single_instance_refire"));

		// Create a random key
		String key = Utility.generateRandomString(8);

		// Subscribe
		pubsub.subscribe("lockchannel:" + key, (message -> {
			PUBSUB.add(message);
			cdl.countDown();
		}));

		// Grab a connection
		try(Jedis jedis = pool.getResource()){
			try{
				// Load the script
				String script_hash = jedis.scriptLoad(SCRIPT_NAME_TO_SCRIPTS.get("single_instance_refire"));

				// Run unlock
				jedis.evalsha(script_hash, 3, key, VERY_LONG_TIMEOUT + "", "");
				assertEquals("Value from Pubsub is messed up", "#", PUBSUB.poll(MARGIN, TimeUnit.MILLISECONDS));
				assertEquals("Lockpoint is gone or messed up.", "dead", jedis.get("lockpoint:" + key));

				// Clean up
				jedis.del("lockpoint:" + key);

				// Add something in lockwait
				jedis.rpush("lockwait:" + key, "something");

				// Run unlock
				jedis.evalsha(script_hash, 3, key, VERY_LONG_TIMEOUT + "", "");
				assertEquals("Value from Pubsub is messed up", "o:something", PUBSUB.poll(MARGIN, TimeUnit.MILLISECONDS));
				assertEquals("Lockpoint is gone or messed up.", "dead", jedis.get("lockpoint:" + key));

				// Run unlock
				Object ret_val = jedis.evalsha(script_hash, 3, key, VERY_LONG_TIMEOUT + "", "");
				assertEquals("Lockpoint is gone or messed up.", "dead", jedis.get("lockpoint:" + key));
				assertTrue("Return value is not an integer!", ret_val instanceof Long);
				assertNotEquals("Return value is not supposed to be 0!", ((Long) ret_val).longValue(), -2);
				assertNotEquals("Return value is not supposed to be -1!", ((Long) ret_val).longValue(), -2);
				assertNotEquals("Return value is not supposed to be -2!", ((Long) ret_val).longValue(), -2);

			}finally{
				jedis.del("lockpoint:" + key);
				jedis.del("lockwait:" + key);
				pubsub.unsubscribe("lockchannel:" + key);
			}
		}
	}
}

