package com.tomansill.redis.lock;

import redis.clients.jedis.Jedis;

import javax.annotation.Nonnull;
import java.io.*;
import java.sql.Time;
import java.util.Objects;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public final class TestUtility {

    public static long LOCK_LEASE = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);

    public static long LOCKWAIT_LEASE = TimeUnit.MILLISECONDS.convert(2, TimeUnit.MINUTES);

    public static long PUB_SUB_MESSAGE_WAIT = TimeUnit.MILLISECONDS.convert(100, TimeUnit.MILLISECONDS);

    /** Single Instance Lock Script */
    private static final String SINGLE_INSTANCE_LOCK_SCRIPT_PATH = "single_instance_lock.lua";

    private static String SINGLE_INSTANCE_LOCK_SCRIPT = null;

    private TestUtility(){
    }

    public static String getSingleInstanceLockScript(){
        if(SINGLE_INSTANCE_LOCK_SCRIPT == null){
            SINGLE_INSTANCE_LOCK_SCRIPT = readInWholeFileInResources(SINGLE_INSTANCE_LOCK_SCRIPT_PATH);
        }
        return SINGLE_INSTANCE_LOCK_SCRIPT;
    }

    private static String readInWholeFileInResources(@Nonnull String path){

        // Open resource
        try(
                InputStream is = TestUtility.class.getClassLoader().getResourceAsStream(path);
                Scanner scanner = new Scanner(Objects.requireNonNull(is))
        ){

            // Set up SB to read in file
            StringBuffer sb = new StringBuffer();

            // Declare line
            String line;

            // Loop until EOL
            while((line = scanner.nextLine()) != null) sb.append(line).append("\n");

            // Return it
            return sb.toString();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String convert(boolean value){
        return value ? "1" : "0";
    }

    public static int performLockScript(
            @Nonnull Jedis connection,
            @Nonnull String script_hash,
            @Nonnull String lockpoint,
            @Nonnull String client_id,
            @Nonnull String lock_id,
            boolean is_fair,
            boolean first_attempt,
            long lock_lease,
            long lockwait_lease,
            boolean is_read,
            @Nonnull String prefix,
            boolean try_lock
    ){

        // Run it and get mystery object
        Object mystery = connection.evalsha(
                script_hash,
                10,
                lockpoint,
                client_id,
                lock_id,
                convert(is_fair),
                convert(first_attempt),
                lock_lease + "",
                lockwait_lease + "",
                convert(is_read),
                prefix,
                convert(try_lock)
        );

        // Attempt to convert it
        if(mystery instanceof Integer) return (int) mystery;

        // Try Long
        if(mystery instanceof Long) return (int) (long) mystery;

        // Fail
        throw new RuntimeException("Unknown value: " + mystery.getClass().getName());
    }

}
