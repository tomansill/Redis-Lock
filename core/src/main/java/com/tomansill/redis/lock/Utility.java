package com.tomansill.redis.lock;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.FileReader;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/** Sensitive data object, used for testing concurrency mechanisms
 *  @author <a href="mailto:tom@ansill.com">Tom Ansill</a>
 */
public final class Utility {

    private Utility(){} // Prevents instantiation

    private static Random RANDOM_SOURCE = null;
    private static Lock RANDOM_SOURCE_LOCK = new ReentrantLock();

    /** Converts byte array to a hexidecimal string
     *  @param digest byte array
     *  @return hexidecimal string
     */
    @Nonnull
    public static String toHex(@Nonnull byte[] digest) {
        char[] mapping = {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};
        char[] new_str = new char[digest.length * 2];
        for(int i = 0; i < digest.length; i++){
            int map_index = digest[i] & 0xFF;
            new_str[i * 2] = mapping[map_index >>> 4];
            new_str[(i * 2) + 1] = mapping[map_index & 0x0F];
        }
        return new String(new_str);
    }

    /** Generates a random byte array
     *  @param length length of byte array
     *  @return byte array of random bytes
     */
    @Nonnull
    public static byte[] generateRandomArray(@Nonnegative int length) throws ExceptionInInitializerError {

        if(RANDOM_SOURCE == null){
            RANDOM_SOURCE_LOCK.lock();
            try{
                if(RANDOM_SOURCE == null){
                    try{
                        RANDOM_SOURCE = SecureRandom.getInstance("NativePRNG");
                    }catch(NoSuchAlgorithmException e) {
                        try{
                            RANDOM_SOURCE = SecureRandom.getInstance("SHA1PRNG");
                        }catch(NoSuchAlgorithmException es) {
                            throw new ExceptionInInitializerError("Failed to obtain SecureRandom instance!");
                        }
                    }
                }
            }finally {
                RANDOM_SOURCE_LOCK.unlock();
            }
        }

        // Get array of random bytes
        byte[] random_array = new byte[length];
        RANDOM_SOURCE.nextBytes(random_array);
        return random_array;
    }

    /** Generates a random string. <B>NOTE:</B> This function is not guaranteed to draw from secure random source
     *  @param length length of string
     *  @return string of random characters in hexidecimal format
     */
    @Nonnull
    public static String generateRandomString(@Nonnegative int length) {
        // Convert to hex and return it
        return Base64.getEncoder().encodeToString(generateRandomArray(3*length)).replace("/", "A").replace("+", "B").substring(0, length);
    }

    public static <T> T checkValueForNull(@Nullable T value, @Nonnull String variable_name) {
    	if(value == null) throw new IllegalArgumentException(variable_name + " is null");
    	return value;
    }

    @Nonnull
    public static String processScript(@Nonnull String script, boolean debug, @Nonnull String debug_output_channel){

        // Split the script by lines
        String[] lines = script.split("\n");

        // Read the script
        StringBuilder new_script = new StringBuilder();

        boolean debug_block = false;
        for(String line : lines){

            //System.out.println(line);

            // Get debug tag
            int debug_index = line.indexOf("--!");
            if (debug_index != -1) {

                // If debug_block is false, expect 'start', otherwise expect 'end'
                if (!debug_block) {
                    if ((debug_index + "--!start".length()) <= line.length() && line.indexOf("start") == (debug_index + 3)) {
                        debug_block = true;
                        continue;
                    }
                } else {
                    if ((debug_index + "--!end".length()) <= line.length() && line.indexOf("end") == (debug_index + 3)) {
                        debug_block = false;
                        continue;
                    }
                }

                line = line.replace("--!", "");
            }

            // If debug is not enabled, don't copy any code in debug block
            if (!debug_block || debug) {

                // Remove all comments
                int comment_index = line.indexOf("--");
                if (comment_index != -1) line = line.substring(0, comment_index);

                // Convert any 'debug_print' to redis pubsub print
                if (debug) {
                    line = line.replace("debug_print(", "redis.call(\"PUBLISH\", \"" + debug_output_channel + "\", ")
                            .trim();
                }

                // Check if it's worth adding into output script
                if (!line.trim().equals("")) {
                    new_script.append(line);
                    new_script.append('\n');
                }
            }
        }

        // Catch trailing debug block
        if (debug_block) throw new RuntimeException("Trailing --!start debug block!");

        // Return script block
        return new_script.toString();
    }
}
