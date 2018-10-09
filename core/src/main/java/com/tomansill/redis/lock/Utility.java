package com.tomansill.redis.lock;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/** Sensitive data object, used for testing concurrency mechanisms
 *  @author <a href="mailto:tom@ansill.com">Tom Ansill</a>
 */
public class Utility{

    private Utility(){} // Prevents instantiation

    private static Random RANDOM_SOURCE = null;
    private static Lock RANDOM_SOURCE_LOCK = new ReentrantLock();

    /** Converts byte array to a hexidecimal string
     *  @param digest byte array
     *  @return hexidecimal string
     */
    public static String toHex(final byte[] digest){
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
    public static byte[] generateRandomArray(final int length) throws ExceptionInInitializerError {

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
    public static String generateRandomString(final int length){
        // Convert to hex and return it
        return Base64.getEncoder().encodeToString(generateRandomArray(3*length)).replace("/", "A").replace("+", "B").substring(0, length);
    }

    public static <T> T checkValueForNull(final T value, final String variable_name){
    	if(value == null) throw new IllegalArgumentException(variable_name + " is null");
    	return value;
    }
}
