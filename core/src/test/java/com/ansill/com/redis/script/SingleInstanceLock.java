package com.ansill.com.redis.script;

import com.ansill.redis.Subscription;
import com.ansill.redis.lock.Message;
import com.ansill.redis.test.MessageQueue;
import com.ansill.redis.test.ServerUtility;
import redis.clients.jedis.Jedis;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.ansill.redis.test.TestUtility.assertType;
import static org.junit.jupiter.api.Assertions.*;

public class SingleInstanceLock implements TestVariables{

    private static final String SCRIPT_PATH = "single_instance_lock.lua";

    private SingleInstanceLock(){
    }

    public static void assertFunction(
            @Nonnull ServerUtility.Server server,
            @Nonnull ScriptManager script_manager,
            @Nonnull Parameters parameters,
            @Nonnull ExpectedOutput output
    ) throws InterruptedException, IOException{

        // Set up queue
        MessageQueue queue = new MessageQueue();

        // Get connection
        try(
                Jedis connection = server.getConnection();
                Subscription ignored = server.getManager()
                                             .subscribe(
                                                     TestVariables.getLockchannel(
                                                             parameters.prefix,
                                                             parameters.lockpoint
                                                     ),
                                                     queue::enqueue
                                             )
        ){

            // Get script hash
            String script_hash = script_manager.loadScript(connection, SCRIPT_PATH);

            // Fire it
            Object return_object = connection.evalsha(
                    script_hash,
                    10,
                    parameters.lockpoint,
                    parameters.client_id,
                    parameters.lock_id,
                    parameters.is_fair ? "1" : "0",
                    parameters.first_attempt ? "1" : "0",
                    parameters.lock_lease + "",
                    parameters.lockwait_lease + "",
                    parameters.is_read ? "1" : "0",
                    parameters.prefix,
                    parameters.trylock ? "1" : "0"
            );

            // Assert not null
            assertNotNull(return_object, "object returned by the script is null!");

            // Assert that object is of Long class
            long result = assertType(Long.class, return_object, "object returned by the script is not a Long class");

            // Assert correct return value
            assertEquals(output.expected_script_return_value, result, "Return value does not match the expected value");

            // Switch on return value
            if(result == 0 || result == -3){
                assertSuccess(parameters, output, connection, queue);
                if(result == -3) assertSharedSuccess(parameters, output, connection);
            }else throw new RuntimeException("Unexpected code");
        }
    }

    private static void assertSharedSuccess(
            @Nonnull Parameters parameters,
            @Nonnull ExpectedOutput output,
            @Nonnull Jedis connection
    ) throws InterruptedException{

        // Check lock count
        assertEquals(
                output.lockcount + "",
                connection.get(TestVariables.getLockcount(parameters.prefix, parameters.lockpoint)),
                "Lockcount is wrong"
        );

    }

    private static void assertSuccess(
            @Nonnull Parameters parameters,
            @Nonnull ExpectedOutput output,
            @Nonnull Jedis connection,
            @Nonnull MessageQueue queue
    ) throws InterruptedException{

        // Check if lockpoint is created
        assertEquals(
                output.expected_lockpoint_value,
                connection.get(TestVariables.getLockpoint(parameters.prefix, parameters.lockpoint)),
                "Lockpoint is either missing or have invalid value"
        );

        // Check if lockpoint's TTL is set up properly
        assertEquals(
                LOCK_LEASE - (PTTL_MARGIN / 2),
                connection.pttl(TestVariables.getLockpoint(parameters.prefix, parameters.lockpoint)),
                PTTL_MARGIN,
                "Lockpoint's TTL value is invalid"
        );

        // Check lock channel for expected message
        Optional<String> q_message = queue.poll(CHANNEL_WAIT_TIME_DURATION, CHANNEL_WAIT_TIME_UNIT);
        assertTrue(q_message.isPresent(), "Timed out waiting for message in lockchannel");

        // Attempt to interpret message
        Optional<Message> o_message = Message.interpret(q_message.get());

        // Assert that read was successful
        assertTrue(o_message.isPresent(), "Message read failed!");

        // Ensure LOCK type
        assertEquals(output.expected_pubsub_message_type, o_message.get().getType(), "Incorrect message type");

        // Ensure correct client id
        assertEquals(Optional.ofNullable(output.client_id), o_message.get().getClientId(), "Incorrect client id");

        // Ensure correct lock lease time
        assertEquals(Optional.ofNullable(output.lock_lease), o_message.get().getLeaseTime(), "Incorrect lease time");

        // Ensure no "side effects"
        assertFalse(
                connection.exists(TestVariables.getLockwait(parameters.prefix, parameters.lockpoint)),
                "Lockwait somehow exists!"
        );
        assertEquals(
                output.lock_count_exists,
                connection.exists(TestVariables.getLockcount(parameters.prefix, parameters.lockpoint)),
                "Lockcount somehow exists!"
        );
        assertFalse(
                connection.exists(TestVariables.getLockpool(parameters.prefix, parameters.lockpoint)),
                "Lockpool somehow exists!"
        );

    }


    public static class Parameters{
        @Nonnull
        private final String lockpoint;
        @Nonnull
        private final String client_id;
        @Nonnull
        private final String lock_id;
        private final boolean is_fair;
        private final boolean first_attempt;
        private final long lock_lease;
        private final long lockwait_lease;
        private final boolean is_read;
        @Nonnull
        private final String prefix;
        private final boolean trylock;

        public Parameters(
                @Nonnull String lockpoint,
                @Nonnull String client_id,
                @Nonnull String lock_id,
                boolean is_fair,
                boolean first_attempt,
                long lock_lease,
                long lockwait_lease,
                boolean is_read,
                @Nonnull String prefix,
                boolean trylock
        ){
            this.lockpoint = lockpoint;
            this.client_id = client_id;
            this.lock_id = lock_id;
            this.is_fair = is_fair;
            this.first_attempt = first_attempt;
            this.lock_lease = lock_lease;
            this.lockwait_lease = lockwait_lease;
            this.is_read = is_read;
            this.prefix = prefix;
            this.trylock = trylock;
        }
    }

    public static class ExpectedOutput{
        @Nonnull
        public final Message.Type expected_pubsub_message_type;
        @Nullable
        public final String client_id;
        @Nullable
        public final Long lock_lease;
        public final boolean lock_count_exists;
        private final long expected_script_return_value;
        @Nonnull
        private final String expected_lockpoint_value;
        private final int lockcount;

        private ExpectedOutput(
                long expected_script_return_value,
                @Nonnull String expected_lockpoint_value,
                @Nonnull Message.Type expected_pubsub_message_type,
                @Nullable String client_id,
                @Nullable Long lock_lease,
                boolean lock_count_exists,
                int lockcount
        ){
            this.expected_script_return_value = expected_script_return_value;
            this.expected_lockpoint_value = expected_lockpoint_value;
            this.expected_pubsub_message_type = expected_pubsub_message_type;
            this.client_id = client_id;
            this.lock_lease = lock_lease;
            this.lock_count_exists = lock_count_exists;
            this.lockcount = lockcount;
        }

        public static ExpectedOutput success(Parameters parameters){
            return new ExpectedOutput(
                    0,
                    parameters.is_read ? "open" : "unique",
                    parameters.is_read ? Message.Type.OPEN : Message.Type.LOCK,
                    parameters.is_read ? null : parameters.client_id,
                    parameters.is_read ? null : parameters.lock_lease,
                    parameters.is_read,
                    -1
            );
        }

        public static ExpectedOutput readSuccess(int lockcount, Parameters parameters){
            return new ExpectedOutput(
                    -3,
                    "open",
                    Message.Type.LOCK,
                    parameters.client_id,
                    parameters.lock_lease,
                    true,
                    lockcount
            );
        }

        public static ExpectedOutput fail(Parameters... parameters){

            // Save success
            Parameters success = parameters[0];

            // Convert all failed
            List<Parameters> failed = Arrays.asList(Arrays.copyOfRange(parameters, 1, parameters.length));

            return null; // TODO continue here
        }
    }
}
