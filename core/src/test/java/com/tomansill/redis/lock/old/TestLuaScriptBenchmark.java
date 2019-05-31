package com.tomansill.redis.lock.old;

import com.tomansill.redis.test.util.JedisPubSubManager;
import com.tomansill.redis.lock.TestUtility;
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
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.tomansill.redis.lock.Utility.generateRandomString;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

public class TestLuaScriptBenchmark{

	private static final String HOSTNAME = "localhost";

	private static final int PORT = 6379;

	private static final HashMap<String, String> SCRIPT_NAME_TO_SCRIPTS = new HashMap<>();

	private static final String[] FILENAMES = {"single_instance_lock", "single_instance_unlock", "single_instance_refire"};

	private static JedisPool POOL = null;

	private static final long VERY_LONG_TIMEOUT = 60000;

	private static final int TEST_LENGTH = 10000;

	private static JedisPubSubManager PUB_SUB = null;

	private static String LOCKPOINT_PREFIX = "lockpoint:";

	private static String LOCKWAIT_PREFIX = "lockwait:";

	private static String LOCKCOUNT_PREFIX = "lockcount:";

	private static String LOCKPOOL_PREFIX = "lockpool:";

	private static Lock LOCK = new ReentrantLock();

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

	private static void compute(Set<ArrayList<Boolean>> set, ArrayList<Boolean> list, int level){

		ArrayList<Boolean> true_list = new ArrayList<>(list);
		ArrayList<Boolean> false_list = new ArrayList<>(list);

		true_list.add(true);
		false_list.add(false);

		if(level == 0){
			set.add(true_list);
			set.add(false_list);
		}else{
			compute(set, true_list, level - 1);
			compute(set, false_list, level - 1);
		}
	}

	@Test
	public void benchmarkLock(){

		// Check if script is available
		assumeTrue("Script name 'single_instance_lock' is not available. We cannot test this", SCRIPT_NAME_TO_SCRIPTS.containsKey("single_instance_lock"));

		// Check if pool is available
		assumeNotNull("Pool is not initialized, we cannot test this.", POOL);


		final Set<ArrayList<Boolean>> test = new HashSet<>();

		compute(test, new ArrayList<>(), 3);

		// Matrix
		boolean[][] booleans = new boolean[test.size()][4];

		int counter = 0;
		for(ArrayList<Boolean> list : test){
			booleans[counter] = new boolean[list.size()];

			for(int i = 0; i < list.size(); i++){
				booleans[counter][i] = list.get(i);
			}

			counter++;
		}

		String str = generateRandomString(4);

		// Grab a connection
		Jedis jedis = POOL.getResource();
		try{

			LOCK.lock();

			// Load the script
			String script_hash = jedis.scriptLoad(SCRIPT_NAME_TO_SCRIPTS.get("single_instance_lock"));

			ArrayList<Long> bin = new ArrayList<>();

			for(int i = 0; i < TEST_LENGTH; i++){
				boolean[] booleans_lst = booleans[i % booleans.length];

				bin.add(performLock(str, true, script_hash, jedis, booleans_lst));
			}

			System.out.println("\n benchmarkLock");
			printStats(bin, "NS");

		}finally{
			jedis.close();
			LOCK.unlock();
		}
	}

	@Test
	public void benchmarkLock1(){

		// Check if script is available
		assumeTrue("Script name 'single_instance_lock' is not available. We cannot test this", SCRIPT_NAME_TO_SCRIPTS.containsKey("single_instance_lock"));

		// Check if pool is available
		assumeNotNull("Pool is not initialized, we cannot test this.", POOL);

		// Matrix
		boolean[][] booleans = new boolean[16][4];

		final Set<ArrayList<Boolean>> test = new HashSet<>();

		compute(test, new ArrayList<>(), 3);

		String lockname = generateRandomString(4);

		int counter = 0;
		for(ArrayList<Boolean> list : test){
			booleans[counter] = new boolean[list.size()];

			for(int i = 0; i < list.size(); i++){
				booleans[counter][i] = list.get(i);
			}

			counter++;
		}

		// Grab a connection
		Jedis jedis = POOL.getResource();
		try{

			LOCK.lock();

			// Load the script
			String script_hash = jedis.scriptLoad(SCRIPT_NAME_TO_SCRIPTS.get("single_instance_lock"));

			ArrayList<Long> bin = new ArrayList<>();

			for(int i = 0; i < TEST_LENGTH; i++){
				boolean[] booleans_lst = booleans[i % booleans.length];

				bin.add(performLock(lockname, false, script_hash, jedis, booleans_lst));
			}

			System.out.println("\n benchmarkLock1 lockpoint name: " + lockname);
			printStats(bin, "NS");

		}finally{
			jedis.expire(LOCKPOINT_PREFIX + lockname, 60);
			jedis.expire(LOCKPOOL_PREFIX + lockname, 60);
			jedis.expire(LOCKWAIT_PREFIX + lockname, 60);
			jedis.expire(LOCKCOUNT_PREFIX + lockname, 60);
			jedis.close();
			LOCK.unlock();
		}
	}

	private long performLock(String lockname, boolean cleanup, String script_name, Jedis jedis, boolean[] booleans){


		try{
			long time = System.currentTimeMillis();

			// Fire the script
			jedis.evalsha(
					script_name,
					10,
					 lockname,
					"client1",
					generateRandomString(4),
					(booleans[0] ? "1" : "0"),
					(booleans[1] ? "1" : "0"),
					100 + "",
					(VERY_LONG_TIMEOUT * 2) + "",
					(booleans[2] ? "1" : "0"),
					"",
					(booleans[3] ? "1" : "0")
			);

			return System.currentTimeMillis() - time;

		}catch(Exception e){
			throw e;
		}finally{
			if(cleanup){
				jedis.del(LOCKPOINT_PREFIX + lockname);
				jedis.del(LOCKPOOL_PREFIX + lockname);
				jedis.del(LOCKWAIT_PREFIX + lockname);
				jedis.del(LOCKCOUNT_PREFIX + lockname);
			}
		}
	}

	private static void printStats(ArrayList<Long> list, String unit){

		double mean = 0.0;
		double variance = 0.0;
		long max = Long.MIN_VALUE;
		long min = Long.MAX_VALUE;

		for(Long val : list){
			max = Math.max(max, val);
			min = Math.min(min, val);
			mean += val;
			variance += Math.pow(val, 2);
		}

		mean /= list.size();

		variance /= list.size();

		double dev = Math.sqrt(variance);

		System.out.println("Size: " + list.size());
		System.out.println("Mean: " + mean + " " + unit);
		System.out.println("Variance: " + variance + " " + unit);
		System.out.println("Deviation: " + dev + " " + unit);
		System.out.println("Max: " + max + " " + unit);
		System.out.println("Min: " + min + " " + unit);
	}
}
