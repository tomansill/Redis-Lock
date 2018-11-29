package com.tomansill.redis.lock;

import com.tomansill.redis.test.util.ResetableCountDownLatch;
import org.junit.AfterClass;
import org.junit.BeforeClass;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ScriptTest{

	private static final String HOSTNAME = "localhost";

	private static final int PORT = 6379;

	private static final HashMap<String, String> SCRIPT_NAME_TO_SCRIPTS = new HashMap<>();

	private static final String[] FILENAMES = {"single_instance_lock", "single_instance_unlock", "single_instance_refire"};

	private static JedisPool REDIS_POOL = null;

	private static final long VERY_LONG_TIMEOUT = 60000;

	private static final long MARGIN = 40;

	private static JedisPubSubManager PUBSUB_MANAGER;

	@BeforeClass
	public static void setUp(){

		// Class loader
		ClassLoader class_loader = ScriptTest.class.getClassLoader();

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

		// Create REDIS_POOL
		REDIS_POOL = new JedisPool(new JedisPoolConfig(), HOSTNAME, PORT);

		// Create pubsub
		PUBSUB_MANAGER = new JedisPubSubManager(REDIS_POOL.getResource());
	}

	@AfterClass
	public static void tearDown(){
		if(PUBSUB_MANAGER != null){
			PUBSUB_MANAGER.close();
			PUBSUB_MANAGER = null;
		}
	}

	@Test
	public void start(){

		// Create a random key
		String lockpoint = Utility.generateRandomString(8);

		// Create subscription message
		BlockingQueue<String> pubsub_messages = new LinkedBlockingQueue<>();
		ResetableCountDownLatch rcdl = new ResetableCountDownLatch(1);

		// Subscribe
		PUBSUB_MANAGER.subscribe("lockchannel:" + lockpoint, (message) -> {
			pubsub_messages.add(message);
			rcdl.countDown();
		});

		// Grab a connection
		try(Jedis jedis = REDIS_POOL.getResource()){

			// Load the script
			String script_hash = jedis.scriptLoad(SCRIPT_NAME_TO_SCRIPTS.get("single_instance_lock"));

		}finally{
			PUBSUB_MANAGER.unsubscribe("lockchannel:" + lockpoint);
		}
	}
}
