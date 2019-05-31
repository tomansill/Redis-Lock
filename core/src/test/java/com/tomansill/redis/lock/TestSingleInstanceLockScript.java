package com.tomansill.redis.lock;

import com.tomansill.redis.test.util.ServerUtility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.concurrent.BlockingQueue;

import static com.tomansill.redis.lock.TestUtility.LOCKWAIT_LEASE;
import static com.tomansill.redis.lock.TestUtility.LOCK_LEASE;
import static com.tomansill.redis.lock.Utility.generateRandomString;
import static org.junit.Assert.assertEquals;

public class TestSingleInstanceLockScript{

    private static final boolean DEBUG = true;

    private final String lockpoint = generateRandomString(8);

    private ServerUtility.Server server;

    private final String debug_channel = generateRandomString(8);

    private String script_reference;

    private BlockingQueue<String> stream;

    @Before
    public void setUp(){

        // Get the server
        this.server = ServerUtility.getServer();

        // Get the script
        String script = TestUtility.getSingleInstanceLockScript();

        // Clean the script
        script = Utility.processScript(script, DEBUG, debug_channel);

        // Get connection
        try(Jedis connection = this.server.getConnection()){

            // Insert the script
            this.script_reference = connection.scriptLoad(script);
        }

        // Get lockchannel stream
        this.stream = this.server.getStream("");
    }

    @After
    public void tearDown(){
        this.server.close();
    }

    @Test
    public void testSimpleUnfairWriteLock(){

        // Obtain connection
        try(Jedis connection = this.server.getConnection()){

            // Create client ID
            String client_id = generateRandomString(8);

            // Create lock id
            String lock_id = generateRandomString(8);

            // Perform it
            int result = TestUtility.performLockScript(
                    connection,
                    script_reference,
                    lockpoint,
                    client_id,
                    lock_id,
                    false,
                    true,
                    LOCK_LEASE,
                    LOCKWAIT_LEASE,
                    false,
                    "",
                    false
            );

            // Should get 0 - success
            assertEquals(0, result); // Todo add message when we get JUnit5

            // Check stream
            //assertNotNull(stream.);
        }

    }

}
