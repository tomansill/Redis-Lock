package com.tomansill.redis.lock;

import com.tomansill.redis.test.util.ResetableCountDownLatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

public class TestLuaScriptForSingleMaster{

	private static final String HOSTNAME = "localhost";

	private static final int PORT = 6379;

	private static final HashMap<String, String> SCRIPT_NAME_TO_SCRIPTS = new HashMap<>();

	private static final String[] FILENAMES = {"single_instance_lock", "single_instance_unlock", "single_instance_refire"};

	private static JedisPool POOL = null;

	private static final long VERY_LONG_TIMEOUT = 60000;

	private static final long TIMEOUT_MARGIN = 100;

	private static final long AWAIT_TIMEOUT = 40;

	private static JedisPubSubManager PUB_SUB = null;

	private static String LOCKPOINT_PREFIX = "lockpoint:";

	private static String LOCKWAIT_PREFIX = "lockwait:";

	private static String LOCKCOUNT_PREFIX = "lockcount:";

	private static String LOCKPOOL_PREFIX = "lockpool:";

	private static String LOCKCHANNEL_PREFIX = "lockchannel:";

	@Before
	public void setUp(){

		// Class loader
		ClassLoader class_loader = getClass().getClassLoader();

		// Load scripts
		for(String filename : FILENAMES){

			// Get file
			File file = new File(Objects.requireNonNull(class_loader.getResource(filename + ".lua")).getFile());

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
		POOL = new JedisPool(new JedisPoolConfig(), HOSTNAME, PORT);

		// Create pubsub
		PUB_SUB = new JedisPubSubManager(POOL.getResource());
	}

	@After
	public void tearDown(){

		if(POOL != null) POOL.close();

		if(PUB_SUB != null) PUB_SUB.unsubscribeAll();

	}

	@Test
	public void testUnfairWriteLock() throws InterruptedException{

		// Check if script is available
		assumeTrue("Script name 'single_instance_lock' is not available. We cannot test this", SCRIPT_NAME_TO_SCRIPTS.containsKey("single_instance_lock"));

		// Check if pool is available
		assumeNotNull("Pool is not initialized, we cannot test this.", POOL);

		// Create a random lockpoint
		String lockname = Utility.generateRandomString(8);

		// Lockchannel
		String lockchannel = LOCKCHANNEL_PREFIX + lockname;

		// Create an queue
		final Queue<String> pubsub_result = new ConcurrentLinkedQueue<>();

		// Create reusable CDL
		final ResetableCountDownLatch rcdl = new ResetableCountDownLatch(0);

		// Grab a connection
		Jedis jedis = POOL.getResource();
		try{

			// Load the script
			String script_hash = jedis.scriptLoad(SCRIPT_NAME_TO_SCRIPTS.get("single_instance_lock"));

			// Subscribe
			PUB_SUB.subscribe(lockchannel, (message) -> {
				pubsub_result.add(message);
				rcdl.countDown();
			});

			// Perform a successful unfair lock
			performExpectedSuccessfulLock(
				jedis, 
				rcdl, 
				pubsub_result,
				script_hash,
				lockname,
				Utility.generateRandomString(4),
				false, 
				false, 
				true, 
				false
			);

			// Perform an unsuccessful unfair lock
			performExpectedUnsuccessfulLock(
				jedis,
				1,
				pubsub_result,
				script_hash,
				lockname,
				Utility.generateRandomString(4),
				false,
				false,
				true,
				false
			);

			// Assert non-existence of lockwait
			assertFalse("Lockwait is not supposed to exist. That's not quite right... ", jedis.exists(LOCKWAIT_PREFIX + lockname));

			// Assert non-existence of lockcount
			assertFalse("Lockcount is not supposed to exist. That's not quite right... ", jedis.exists(LOCKCOUNT_PREFIX + lockname));

			// Assert non-existence of lockpool
			assertFalse("Lockpool is not supposed to exist. That's not quite right... ", jedis.exists(LOCKPOOL_PREFIX + lockname));

		}finally{
			cleanup(jedis, lockname);
			PUB_SUB.unsubscribe(lockchannel);
			jedis.close();
		}
	}

	@Test
	public void testFairWriteLock() throws InterruptedException{

		// Check if script is available
		assumeTrue("Script name 'single_instance_lock' is not available. We cannot test this", SCRIPT_NAME_TO_SCRIPTS.containsKey("single_instance_lock"));

		// Check if pool is available
		assumeNotNull("Pool is not initialized, we cannot test this.", POOL);

		// Create a random lockpoint
		String lockname = Utility.generateRandomString(8);

		// Lockchannel
		String lockchannel = LOCKCHANNEL_PREFIX + lockname;

		// Create an queue
		final Queue<String> pubsub_result = new ConcurrentLinkedQueue<>();

		// Create reusable CDL
		final ResetableCountDownLatch rcdl = new ResetableCountDownLatch(0);

		// Grab a connection
		Jedis jedis = POOL.getResource();
		try{

			// Load the script
			String script_hash = jedis.scriptLoad(SCRIPT_NAME_TO_SCRIPTS.get("single_instance_lock"));

			// Subscribe
			PUB_SUB.subscribe(lockchannel, (message) -> {
				pubsub_result.add(message);
				rcdl.countDown();
			});

			// Perform a successful fair lock
			performExpectedSuccessfulLock(
					jedis,
					rcdl,
					pubsub_result,
					script_hash,
					lockname,
					Utility.generateRandomString(4),
					false,
					true,
					true,
					false
			);

			// Perform an unsuccessful fair lock
			performExpectedUnsuccessfulLock(
					jedis,
					1,
					pubsub_result,
					script_hash,
					lockname,
					Utility.generateRandomString(4),
					false,
					true,
					true,
					false
			);

			// Perform an unsuccessful fair lock
			performExpectedUnsuccessfulLock(
					jedis,
					2,
					pubsub_result,
					script_hash,
					lockname,
					Utility.generateRandomString(4),
					false,
					true,
					true,
					false
			);

			// Assert existence of lockwait
			assertTrue("Lockwait does not exist. That's not quite right... ", jedis.exists(LOCKWAIT_PREFIX + lockname));

			// Assert non-existence of lockcount
			assertFalse("Lockcount is not supposed to exist. That's not quite right... ", jedis.exists(LOCKCOUNT_PREFIX + lockname));

			// Assert non-existence of lockpool
			assertFalse("Lockpool is not supposed to exist. That's not quite right... ", jedis.exists(LOCKPOOL_PREFIX + lockname));

		}finally{
			cleanup(jedis, lockname);
			PUB_SUB.unsubscribe(lockchannel);
			jedis.close();
		}
	}

	@Test
	public void testUnfairWriteLockOnFairWriteLock() throws InterruptedException{

		// Check if script is available
		assumeTrue("Script name 'single_instance_lock' is not available. We cannot test this", SCRIPT_NAME_TO_SCRIPTS.containsKey("single_instance_lock"));

		// Check if pool is available
		assumeNotNull("Pool is not initialized, we cannot test this.", POOL);

		// Create a random lockpoint
		String lockname = Utility.generateRandomString(8);

		// Lockchannel
		String lockchannel = LOCKCHANNEL_PREFIX + lockname;

		// Create an queue
		final Queue<String> pubsub_result = new ConcurrentLinkedQueue<>();

		// Create reusable CDL
		final ResetableCountDownLatch rcdl = new ResetableCountDownLatch(0);

		// Grab a connection
		Jedis jedis = POOL.getResource();
		try{

			// Load the script
			String script_hash = jedis.scriptLoad(SCRIPT_NAME_TO_SCRIPTS.get("single_instance_lock"));

			// Subscribe
			PUB_SUB.subscribe(lockchannel, (message) -> {
				pubsub_result.add(message);
				rcdl.countDown();
			});

			// Perform a successful fair lock
			performExpectedSuccessfulLock(
					jedis,
					rcdl,
					pubsub_result,
					script_hash,
					lockname,
					Utility.generateRandomString(4),
					false,
					true,
					true,
					false
			);

			// Perform an unsuccessful unfair lock
			performExpectedUnsuccessfulLock(
					jedis,
					1,
					pubsub_result,
					script_hash,
					lockname,
					Utility.generateRandomString(4),
					false,
					false,
					true,
					false
			);

			// Assert non-existence of lockwait
			assertFalse("Lockwait is not supposed to exist. That's not quite right... ", jedis.exists(LOCKWAIT_PREFIX + lockname));

			// Assert non-existence of lockcount
			assertFalse("Lockcount is not supposed to exist. That's not quite right... ", jedis.exists(LOCKCOUNT_PREFIX + lockname));

			// Assert non-existence of lockpool
			assertFalse("Lockpool is not supposed to exist. That's not quite right... ", jedis.exists(LOCKPOOL_PREFIX + lockname));

		}finally{
			cleanup(jedis, lockname);
			PUB_SUB.unsubscribe(lockchannel);
			jedis.close();
		}
	}

	@Test
	public void testFairWriteLockOnUnfairWriteLock() throws InterruptedException{

		// Check if script is available
		assumeTrue("Script name 'single_instance_lock' is not available. We cannot test this", SCRIPT_NAME_TO_SCRIPTS.containsKey("single_instance_lock"));

		// Check if pool is available
		assumeNotNull("Pool is not initialized, we cannot test this.", POOL);

		// Create a random lockpoint
		String lockname = Utility.generateRandomString(8);

		// Lockchannel
		String lockchannel = LOCKCHANNEL_PREFIX + lockname;

		// Create an queue
		final Queue<String> pubsub_result = new ConcurrentLinkedQueue<>();

		// Create reusable CDL
		final ResetableCountDownLatch rcdl = new ResetableCountDownLatch(0);

		// Grab a connection
		Jedis jedis = POOL.getResource();
		try{

			// Load the script
			String script_hash = jedis.scriptLoad(SCRIPT_NAME_TO_SCRIPTS.get("single_instance_lock"));

			// Subscribe
			PUB_SUB.subscribe(lockchannel, (message) -> {
				pubsub_result.add(message);
				rcdl.countDown();
			});

			// Perform a successful unfair lock
			performExpectedSuccessfulLock(
					jedis,
					rcdl,
					pubsub_result,
					script_hash,
					lockname,
					Utility.generateRandomString(4),
					false,
					false,
					true,
					false
			);

			// Perform an unsuccessful fair lock
			performExpectedUnsuccessfulLock(
					jedis,
					1,
					pubsub_result,
					script_hash,
					lockname,
					Utility.generateRandomString(4),
					false,
					true,
					true,
					false
			);

			// Assert existence of lockwait
			assertTrue("Lockwait does not exist. That's not quite right... ", jedis.exists(LOCKWAIT_PREFIX + lockname));

			// Assert non-existence of lockcount
			assertFalse("Lockcount is not supposed to exist. That's not quite right... ", jedis.exists(LOCKCOUNT_PREFIX + lockname));

			// Assert non-existence of lockpool
			assertFalse("Lockpool is not supposed to exist. That's not quite right... ", jedis.exists(LOCKPOOL_PREFIX + lockname));

		}finally{
			cleanup(jedis, lockname);
			PUB_SUB.unsubscribe(lockchannel);
			jedis.close();
		}
	}

	@Test
	public void testUnfairReadLock() throws InterruptedException{

		// Check if script is available
		assumeTrue("Script name 'single_instance_lock' is not available. We cannot test this", SCRIPT_NAME_TO_SCRIPTS.containsKey("single_instance_lock"));

		// Check if pool is available
		assumeNotNull("Pool is not initialized, we cannot test this.", POOL);

		// Create a random lockpoint
		String lockname = Utility.generateRandomString(8);

		// Lockchannel
		String lockchannel = LOCKCHANNEL_PREFIX + lockname;

		// Create an queue
		final Queue<String> pubsub_result = new ConcurrentLinkedQueue<>();

		// Create reusable CDL
		final ResetableCountDownLatch rcdl = new ResetableCountDownLatch(0);

		// Grab a connection
		Jedis jedis = POOL.getResource();
		try{

			// Load the script
			String script_hash = jedis.scriptLoad(SCRIPT_NAME_TO_SCRIPTS.get("single_instance_lock"));

			// Subscribe
			PUB_SUB.subscribe(lockchannel, (message) -> {
				pubsub_result.add(message);
				rcdl.countDown();
			});

			// Perform a successful unfair lock
			performExpectedSuccessfulLock(
					jedis,
					rcdl,
					pubsub_result,
					1,
					true,
					script_hash,
					lockname,
					Utility.generateRandomString(4),
					true,
					false,
					true,
					false
			);

			// Perform an unsuccessful unfair lock
			performExpectedSuccessfulLock(
					jedis,
					rcdl,
					pubsub_result,
					2,
					false,
					script_hash,
					lockname,
					Utility.generateRandomString(4),
					true,
					false,
					true,
					false
			);

			// Assert non-existence of lockwait
			assertFalse("Lockwait is not supposed to exist. That's not quite right... ", jedis.exists(LOCKWAIT_PREFIX + lockname));

			// Assert existence of lockcount
			assertTrue("Lockcount is supposed to exist. That's not quite right... ", jedis.exists(LOCKCOUNT_PREFIX + lockname));

			// Assert non-existence of lockpool
			assertFalse("Lockpool is not supposed to exist. That's not quite right... ", jedis.exists(LOCKPOOL_PREFIX + lockname));

		}finally{
			cleanup(jedis, lockname);
			PUB_SUB.unsubscribe(lockchannel);
			jedis.close();
		}
	}

	@Test
	public void testFairReadLock() throws InterruptedException{

		// Check if script is available
		assumeTrue("Script name 'single_instance_lock' is not available. We cannot test this", SCRIPT_NAME_TO_SCRIPTS.containsKey("single_instance_lock"));

		// Check if pool is available
		assumeNotNull("Pool is not initialized, we cannot test this.", POOL);

		// Create a random lockpoint
		String lockname = Utility.generateRandomString(8);

		// Lockchannel
		String lockchannel = LOCKCHANNEL_PREFIX + lockname;

		// Create an queue
		final Queue<String> pubsub_result = new ConcurrentLinkedQueue<>();

		// Create reusable CDL
		final ResetableCountDownLatch rcdl = new ResetableCountDownLatch(0);

		// Grab a connection
		Jedis jedis = POOL.getResource();
		try{

			// Load the script
			String script_hash = jedis.scriptLoad(SCRIPT_NAME_TO_SCRIPTS.get("single_instance_lock"));

			// Subscribe
			PUB_SUB.subscribe(lockchannel, (message) -> {
				pubsub_result.add(message);
				rcdl.countDown();
			});

			// Perform a successful fair lock
			performExpectedSuccessfulLock(
					jedis,
					rcdl,
					pubsub_result,
					1,
					true,
					script_hash,
					lockname,
					Utility.generateRandomString(4),
					true,
					true,
					true,
					false
			);

			// Perform an unsuccessful unfair lock
			performExpectedSuccessfulLock(
					jedis,
					rcdl,
					pubsub_result,
					2,
					false,
					script_hash,
					lockname,
					Utility.generateRandomString(4),
					true,
					true,
					true,
					false
			);

			// Assert non-existence of lockwait
			assertFalse("Lockwait is not supposed to exist. That's not quite right... ", jedis.exists(LOCKWAIT_PREFIX + lockname));

			// Assert existence of lockcount
			assertTrue("Lockcount is supposed to exist. That's not quite right... ", jedis.exists(LOCKCOUNT_PREFIX + lockname));

			// Assert non-existence of lockpool
			assertFalse("Lockpool is not supposed to exist. That's not quite right... ", jedis.exists(LOCKPOOL_PREFIX + lockname));

		}finally{
			cleanup(jedis, lockname);
			PUB_SUB.unsubscribe(lockchannel);
			jedis.close();
		}
	}

	@Test
	public void testUnfairReadLockOnFairReadLock() throws InterruptedException{

		// Check if script is available
		assumeTrue("Script name 'single_instance_lock' is not available. We cannot test this", SCRIPT_NAME_TO_SCRIPTS.containsKey("single_instance_lock"));

		// Check if pool is available
		assumeNotNull("Pool is not initialized, we cannot test this.", POOL);

		// Create a random lockpoint
		String lockname = Utility.generateRandomString(8);

		// Lockchannel
		String lockchannel = LOCKCHANNEL_PREFIX + lockname;

		// Create an queue
		final Queue<String> pubsub_result = new ConcurrentLinkedQueue<>();

		// Create reusable CDL
		final ResetableCountDownLatch rcdl = new ResetableCountDownLatch(0);

		// Grab a connection
		Jedis jedis = POOL.getResource();
		try{

			// Load the script
			String script_hash = jedis.scriptLoad(SCRIPT_NAME_TO_SCRIPTS.get("single_instance_lock"));

			// Subscribe
			PUB_SUB.subscribe(lockchannel, (message) -> {
				pubsub_result.add(message);
				rcdl.countDown();
			});

			// Perform a successful fair lock
			performExpectedSuccessfulLock(
					jedis,
					rcdl,
					pubsub_result,
					1,
					true,
					script_hash,
					lockname,
					Utility.generateRandomString(4),
					true,
					true,
					true,
					false
			);

			// Perform an unsuccessful unfair lock
			performExpectedSuccessfulLock(
					jedis,
					rcdl,
					pubsub_result,
					2,
					false,
					script_hash,
					lockname,
					Utility.generateRandomString(4),
					true,
					false,
					true,
					false
			);

			// Assert non-existence of lockwait
			assertFalse("Lockwait is not supposed to exist. That's not quite right... ", jedis.exists(LOCKWAIT_PREFIX + lockname));

			// Assert existence of lockcount
			assertTrue("Lockcount is supposed to exist. That's not quite right... ", jedis.exists(LOCKCOUNT_PREFIX + lockname));

			// Assert non-existence of lockpool
			assertFalse("Lockpool is not supposed to exist. That's not quite right... ", jedis.exists(LOCKPOOL_PREFIX + lockname));

		}finally{
			cleanup(jedis, lockname);
			PUB_SUB.unsubscribe(lockchannel);
			jedis.close();
		}
	}

	@Test
	public void testFairReadLockOnUnfairReadLock() throws InterruptedException{

		// Check if script is available
		assumeTrue("Script name 'single_instance_lock' is not available. We cannot test this", SCRIPT_NAME_TO_SCRIPTS.containsKey("single_instance_lock"));

		// Check if pool is available
		assumeNotNull("Pool is not initialized, we cannot test this.", POOL);

		// Create a random lockpoint
		String lockname = Utility.generateRandomString(8);

		// Lockchannel
		String lockchannel = LOCKCHANNEL_PREFIX + lockname;

		// Create an queue
		final Queue<String> pubsub_result = new ConcurrentLinkedQueue<>();

		// Create reusable CDL
		final ResetableCountDownLatch rcdl = new ResetableCountDownLatch(0);

		// Grab a connection
		Jedis jedis = POOL.getResource();
		try{

			// Load the script
			String script_hash = jedis.scriptLoad(SCRIPT_NAME_TO_SCRIPTS.get("single_instance_lock"));

			// Subscribe
			PUB_SUB.subscribe(lockchannel, (message) -> {
				pubsub_result.add(message);
				rcdl.countDown();
			});

			// Perform a successful fair lock
			performExpectedSuccessfulLock(
					jedis,
					rcdl,
					pubsub_result,
					1,
					true,
					script_hash,
					lockname,
					Utility.generateRandomString(4),
					true,
					true,
					true,
					false
			);

			// Perform an unsuccessful unfair lock
			performExpectedSuccessfulLock(
					jedis,
					rcdl,
					pubsub_result,
					2,
					false,
					script_hash,
					lockname,
					Utility.generateRandomString(4),
					true,
					false,
					true,
					false
			);

			// Assert non-existence of lockwait
			assertFalse("Lockwait is not supposed to exist. That's not quite right... ", jedis.exists(LOCKWAIT_PREFIX + lockname));

			// Assert existence of lockcount
			assertTrue("Lockcount is supposed to exist. That's not quite right... ", jedis.exists(LOCKCOUNT_PREFIX + lockname));

			// Assert non-existence of lockpool
			assertFalse("Lockpool is not supposed to exist. That's not quite right... ", jedis.exists(LOCKPOOL_PREFIX + lockname));

		}finally{
			cleanup(jedis, lockname);
			PUB_SUB.unsubscribe(lockchannel);
			jedis.close();
		}
	}

	private void performExpectedSuccessfulLock(Jedis jedis, ResetableCountDownLatch rcdl, Queue<String> pubsub_result, String script_name, String lockpoint, String lock_name, boolean is_read_lock, boolean is_fair, boolean first_attempt, boolean is_try_lock) throws InterruptedException{
		this.performExpectedSuccessfulLock(jedis, rcdl, pubsub_result, 0, false, script_name, lockpoint, lock_name, is_read_lock, is_fair, first_attempt, is_try_lock);
	}

	private void performExpectedSuccessfulLock(Jedis jedis, ResetableCountDownLatch rcdl, Queue<String> pubsub_result, long expected_lockcount, boolean lock_owner, String script_name, String lockpoint, String lock_name, boolean is_read_lock, boolean is_fair, boolean first_attempt, boolean is_try_lock) throws InterruptedException{

		// Reset the CDL
		rcdl.reset(lock_owner ? 2 : 1);
		
		// Fire the script
		Object return_value = jedis.evalsha(
			script_name,
			10,
			lockpoint,
			"client1",
			lock_name,
			(is_fair ? "1" : "0"),
			(first_attempt ? "1" : "0"),
			VERY_LONG_TIMEOUT + "",
			(VERY_LONG_TIMEOUT * 2) + "",
			(is_read_lock ? "1" : "0"),
			"",
			(is_try_lock ? "1" : "0")
		);
		
		// Assert on type
		Long code = assertType(Long.class, return_value);
		
		// Assert on code
		assertEquals("Return code is not zero!", 0, code.longValue());
		
		// Block until message is received on pubsub or time out
		assertTrue("The message never came!", rcdl.await(AWAIT_TIMEOUT, TimeUnit.MILLISECONDS));
		
		// Assert on pubsub notification
		if(!lock_owner) assertEquals("Message Queue is expected to have only one message on it. ", 1, pubsub_result.size());
		else assertEquals("Message Queue is expected to have two message on it. ", 2, pubsub_result.size());

		// Assert on the shared message
		if(lock_owner) assertEquals("Message from Message Queue is not correct. ", "o:" + lockpoint, pubsub_result.poll());

		// Assert on the lock message
		assertEquals("Message from Message Queue is not correct. ", "l:client1:" + lock_name + ":" + VERY_LONG_TIMEOUT + ":" + lockpoint, pubsub_result.poll());
		
		// Assert on lock existence
		assertTrue("Lockpoint doesn't exist. That's not quite right... ", jedis.exists(LOCKPOINT_PREFIX + lockpoint));
		
		// Assert on lock lifetime
		assertEquals("Lockpoint's TTL is not quite right. ", VERY_LONG_TIMEOUT - TIMEOUT_MARGIN, jedis.pttl(LOCKPOINT_PREFIX + lockpoint), TIMEOUT_MARGIN);
		
		// Assert on lock value
		assertEquals("Lockpoint's value is not correct. ", (is_read_lock ? "open" : "unique"), jedis.get(LOCKPOINT_PREFIX + lockpoint));

		// Read lock stuff
		if(is_read_lock){

			// Assert on lockcount existence
			assertTrue("Lockcount doesn't exist. That's not quite right... ", jedis.exists(LOCKCOUNT_PREFIX + lockpoint));

			// Assert on lockcount lifetime
			assertEquals("Lockcount's TTL is not quite right. ", VERY_LONG_TIMEOUT - TIMEOUT_MARGIN, jedis.pttl(LOCKCOUNT_PREFIX + lockpoint), TIMEOUT_MARGIN);

			// Assert on lockcount type
			long count = Long.parseLong(assertType(String.class, jedis.get(LOCKCOUNT_PREFIX + lockpoint)));

			// Assert on lockcount value
			assertEquals("Lockcount's value is not correct. ", expected_lockcount, count);
		}

		// Assert on pubsub notification
		assertEquals("Message Queue should be empty at this point...", 0, pubsub_result.size());
	}

	@Test
	public void testUnfairReadLockOnUnfairWriteLock() throws InterruptedException{

		// Check if script is available
		assumeTrue("Script name 'single_instance_lock' is not available. We cannot test this", SCRIPT_NAME_TO_SCRIPTS.containsKey("single_instance_lock"));

		// Check if pool is available
		assumeNotNull("Pool is not initialized, we cannot test this.", POOL);

		// Create a random lockpoint
		String lockname = Utility.generateRandomString(8);

		// Lockchannel
		String lockchannel = LOCKCHANNEL_PREFIX + lockname;

		// Create an queue
		final Queue<String> pubsub_result = new ConcurrentLinkedQueue<>();

		// Create reusable CDL
		final ResetableCountDownLatch rcdl = new ResetableCountDownLatch(0);

		// Grab a connection
		Jedis jedis = POOL.getResource();
		try{

			// Load the script
			String script_hash = jedis.scriptLoad(SCRIPT_NAME_TO_SCRIPTS.get("single_instance_lock"));

			// Subscribe
			PUB_SUB.subscribe(lockchannel, (message) -> {
				pubsub_result.add(message);
				rcdl.countDown();
			});

			// Perform a successful fair write lock
			performExpectedSuccessfulLock(
					jedis,
					rcdl,
					pubsub_result,
					script_hash,
					lockname,
					Utility.generateRandomString(4),
					false,
					true,
					true,
					false
			);

			// Perform an unsuccessful fair lock
			performExpectedUnsuccessfulLock(
					jedis,
					1,
					1,
					pubsub_result,
					script_hash,
					lockname,
					Utility.generateRandomString(4),
					true,
					true,
					true,
					false
			);

			// Assert existence of lockwait
			assertTrue("Lockwait is supposed to exist. That's not quite right... ", jedis.exists(LOCKWAIT_PREFIX + lockname));

			// Assert non-existence of lockcount
			assertFalse("Lockcount is not supposed to exist. That's not quite right... ", jedis.exists(LOCKCOUNT_PREFIX + lockname));

			// Assert existence of lockpool
			assertTrue("Lockpool is supposed to exist. That's not quite right... ", jedis.exists(LOCKPOOL_PREFIX + lockname));

		}finally{
			cleanup(jedis, lockname);
			PUB_SUB.unsubscribe(lockchannel);
			jedis.close();
		}
	}

	private void performExpectedUnsuccessfulLock(
		Jedis jedis,
		long expected_number_in_lockwait,
		Queue<String> pubsub_result,
		String script_name,
		String lockpoint,
		String lock_name,
		boolean is_read_lock,
		boolean is_fair,
		boolean first_attempt,
		boolean is_try_lock
	){
		this.performExpectedUnsuccessfulLock(
			jedis,
			expected_number_in_lockwait,
			0,
			pubsub_result, script_name,
			lockpoint, lock_name,
			is_read_lock, is_fair,
			first_attempt,
			is_try_lock
		);
	}

	private void performExpectedUnsuccessfulLock(
		Jedis jedis,
		long expected_number_in_lockwait,
		long expected_number_in_lockpool,
		Queue<String> pubsub_result,
		String script_name,
		String lockpoint,
		String lock_name,
		boolean is_read_lock,
		boolean is_fair,
		boolean first_attempt,
		boolean is_try_lock
	){
		
		// Fire the script
		Object return_value = jedis.evalsha(
			script_name,
			10,
			lockpoint,
			"client1",
			lock_name,
			(is_fair ? "1" : "0"),
			(first_attempt ? "1" : "0"),
			VERY_LONG_TIMEOUT + "",
			(VERY_LONG_TIMEOUT * 2) + "",
			(is_read_lock ? "1" : "0"),
			"",
			(is_try_lock ? "1" : "0")
		);

		// Assert on type
		Long expire = assertType(Long.class, return_value);

		// Assert on code
		assertEquals("Expiration time is nowhere near expected value!", VERY_LONG_TIMEOUT - TIMEOUT_MARGIN, expire, TIMEOUT_MARGIN);

		// Assert on lock existence
		assertTrue("Lockpoint doesn't exist. That's not quite right... ", jedis.exists(LOCKPOINT_PREFIX + lockpoint));
		
		// Make sure the return value is not lying to us
		assertEquals("Expiration time value is very different from actual time on server", jedis.pttl(LOCKPOINT_PREFIX + lockpoint), expire - TIMEOUT_MARGIN, TIMEOUT_MARGIN);

		// Assert lockwait on fair locks
		if(is_fair){

			// Assert on lockwait existence
			assertTrue("Lockwait doesn't exist. That's not quite right... ", jedis.exists(LOCKWAIT_PREFIX + lockpoint));

			// Assert on lockwait lifetime
			assertEquals("Lockwait's TTL is not quite right. ", (VERY_LONG_TIMEOUT * 2) - TIMEOUT_MARGIN, jedis.pttl(LOCKWAIT_PREFIX + lockpoint), TIMEOUT_MARGIN);

			// Assert on lockwait length
			assertEquals("Lockwait's length is not quite right", expected_number_in_lockwait, jedis.llen(LOCKWAIT_PREFIX + lockpoint).longValue());

			// Assert the member on rear of the lockwait
			assertEquals("The member on the rear of the lockwait is not correct, ", (is_read_lock ? "S" : "client1:" + lock_name), jedis.lindex(LOCKWAIT_PREFIX + lockpoint, -1));

			// Read lock
			if(is_read_lock){

				// Assert on lockpool existence
				assertTrue("Lockpool doesn't exist. That's not quite right... ", jedis.exists(LOCKPOOL_PREFIX + lockpoint));

				// Assert on lockpool lifetime
				assertEquals("Lockpool's TTL is not quite right. ", (VERY_LONG_TIMEOUT * 2) - TIMEOUT_MARGIN, jedis.pttl(LOCKPOOL_PREFIX + lockpoint), TIMEOUT_MARGIN);

				// Assert on lockpool length
				assertEquals("Lockpool's length is not quite right", expected_number_in_lockpool, jedis.scard(LOCKPOOL_PREFIX + lockpoint).longValue());

				// Assert the member on rear of the lockpool
				assertTrue("The member does not exist on the lockpool. ", jedis.sismember(LOCKPOOL_PREFIX + lockpoint, "client1:" + lock_name));
			}
		}

		// Pubsub is supposed to be empty
		assertTrue("Pub sub is not supposed to be sending messages. ", pubsub_result.isEmpty());
	}
	
	@SuppressWarnings("unchecked")
	private <T> T assertType(Class<T> clazz, Object mystery_object){

		Class mystery_clazz = mystery_object.getClass();

		// Assert
		assertEquals("The object is not of " + clazz.getSimpleName() + " type", clazz, mystery_clazz);
		
		// Return
		return (T) mystery_object;
	}

	private void cleanup(Jedis jedis, String lockname){
		jedis.del(LOCKPOINT_PREFIX + lockname);
		jedis.del(LOCKPOOL_PREFIX + lockname);
		jedis.del(LOCKWAIT_PREFIX + lockname);
		jedis.del(LOCKCOUNT_PREFIX + lockname);
	}
}
