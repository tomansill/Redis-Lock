package com.tomansill.redis.lock;

import com.tomansill.redis.test.util.TestFunction;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class TestSingleInstance{

	public final static String HOSTNAME = "localhost";
	public final static int PORT = 6379;
	private static AbstractRedisLockClient client = null;

	private TestSingleInstance(){
	}

	public static void setUp(AbstractRedisLockClient in_client){

		assertNotNull("in_client is null!", in_client);

		client = in_client;
	}

	public static void testWriteLock(final boolean fair, final boolean debug){

		// Check database connection
		assumeTrue("We are not connected to Redis server, this test cannot continue.", client != null);

		// Set timeout duration
		long timeout = 5;
		TimeUnit unit = TimeUnit.SECONDS;
		long time;

		// Do control test
		if(debug) System.out.println("Doing control test number one");
		time = System.nanoTime();
		assertTrue(
				"The control test number one has failed, the test is flawed.",
				!TestFunction.performSimpleWriteLock(null, timeout, unit, debug)
		);
		if(debug) System.out.println("Control test number one passed - took: " + ((System.nanoTime() - time)/1000000.0));

		// Do control test
		if(debug) System.out.println("Doing control test number two");
		time = System.nanoTime();
		assertTrue(
				"The control test number two has failed, the test is flawed.",
				TestFunction.performSimpleWriteLock(new ReentrantReadWriteLock(), timeout, unit, debug)
		);
		if(debug) System.out.println("Control test number two passed - took: " + ((System.nanoTime() - time)/1000000.0));

		// Do experiment test with unfair locking
		if(debug) System.out.println("Doing unfair experiment test");
		time = System.nanoTime();
		assertTrue(
				"The unfair experiment test has failed.",
				TestFunction.performSimpleWriteLock(client.getLock(Utility.generateRandomString(8), fair),
				                                    timeout,
				                                    unit,
				                                    debug
				)
		);
		if(debug) System.out.println("Unfair experiment test passed - took: " + ((System.nanoTime() - time)/1000000.0));
	}

	public static void testReadLock(final boolean fair, final boolean debug){

		// Check database connection
		assumeTrue("We are not connected to Redis server, this test cannot continue.", client != null);

		// Set timeout duration
		long timeout = 10;
		TimeUnit unit = TimeUnit.SECONDS;

		// Do control test
		if(debug) System.out.println("Doing control test number one");
		assertTrue(
				"The control test number one has failed, the test is flawed.",
				!TestFunction.performSimpleWriteLock(null, timeout, unit, debug)
		);
		if(debug) System.out.println("Control test number one passed");

		// Do control test
		if(debug) System.out.println("Doing control test number two");
		assertTrue(
				"The control test number two has failed, the test is flawed.",
				TestFunction.performSimpleWriteLock(new ReentrantReadWriteLock(), timeout, unit, debug)
		);
		if(debug) System.out.println("Control test number two passed");

		// Do experiment test with unfair locking
		if(debug) System.out.println("Doing unfair experiment test");
		assertTrue(
				"The unfair experiment test has failed.",
				TestFunction.performSimpleReadLock(client.getLock(Utility.generateRandomString(8), fair),
				                                    timeout,
				                                    unit,
				                                    debug
				)
		);
		if(debug) System.out.println("Unfair experiment test passed");
	}


	public static void testMultipleWriteLocksUnfairLock(final boolean debug){

		// Check database connection
		assumeTrue("We are not connected to Redis server, this test cannot continue.", client != null);

		// Set num threads
		int num_threads = 3;

		// Do control test
		assertTrue(
				"The control test number one has failed, the test is flawed.",
				!TestFunction.performMultipleWriteLock(null, num_threads, debug)
		);

		// Do control test
		assertTrue(
				"The control test number two has failed, the test is flawed.",
				TestFunction.performMultipleWriteLock(new ReentrantReadWriteLock(), num_threads, debug)
		);

		// Do experiment test with unfair locking
		assertTrue(
				"The unfair experiment test has failed.",
				TestFunction.performMultipleWriteLock(
						client.getLock(Utility.generateRandomString(8), false),
						num_threads,
						100,
						TimeUnit.SECONDS,
						debug
				)
		);

		// Do experiment test with fair locking
		assertTrue(
				"The fair experiment test has failed.",
				TestFunction.performMultipleWriteLock(
						client.getLock(Utility.generateRandomString(8), true),
						num_threads,
						100,
						TimeUnit.SECONDS,
						debug
				)
		);

	}

	public static void testMultipleWriteLocksFairLock(final boolean debug){

		// Check database connection
		assumeTrue("We are not connected to Redis server, this test cannot continue.", client != null);

		// Set num threads
		int num_threads = 20;

		// Do control test
		assertTrue(
				"The control test number two has failed, the test is flawed.",
				TestFunction.performMultipleFairLock(new ReentrantReadWriteLock(true), num_threads, debug)
		);

		// Do experiment test with fair locking
		assertTrue(
				"The fair experiment test has failed.",
				TestFunction.performMultipleFairLock(
						client.getLock(Utility.generateRandomString(8), true),
						num_threads,
						5,
						TimeUnit.SECONDS,
						debug
				)
		);

	}
}