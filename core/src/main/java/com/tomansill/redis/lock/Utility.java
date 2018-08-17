package com.tomansill.redis.lock;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Random;

/** Sensitive data object, used for testing concurrency mechanisms
 *  @author <a href="mailto:tom@ansill.com">Tom Ansill</a>
 */
public class Utility{

    private Utility(){} // Prevents instantiation

    private static Random RANDOM_SOURCE = null;

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
     *  @param secure true use only securerandom source, false to allow usage of non-secure random source if secure random
     *                source cannot be obtained
     *  @return byte array of random bytes
     */
    public static byte[] generateRandomArray(final int length, final boolean secure) throws NoSuchAlgorithmException {

        // Initialize random object if its not initialized
        // If secure random is requested and random object is initalized but not secure, then re-initialize it with secure source
        if(RANDOM_SOURCE == null || (secure && !(RANDOM_SOURCE instanceof SecureRandom))){

            // Secure or non-secure, try obtain secure first
            try{
                RANDOM_SOURCE = SecureRandom.getInstanceStrong();
            }catch(NoSuchAlgorithmException nsae){
                if(secure) throw nsae;
                RANDOM_SOURCE = new Random();
            }
        }

        /*
        // SecureRandom may break by "blocking" even though it has been created successfully. We test it by
        // launching future and wait for it to succeed
        if(RANDOM_SOURCE instanceof SecureRandom) {

            // Create future object
            Future<byte[]> f = Executors.newCachedThreadPool().submit(() -> {

                // Create array
                byte[] array = new byte[length];

                // Get random bytes
                RANDOM_SOURCE.nextBytes(array);

                // Return it
                return array;
            });

            // Run the future and limit the execution time to 1 second which is plenty for SecureRandom to do its own job
            // If that time has passed, then SecureRandom is definitely broken
            try{
                return f.get(1, TimeUnit.SECONDS);
            }catch(TimeoutException te){

                // Debug
                System.err.println("SecureRandom.nextByte() timed out!");

                // If secure is required, then there's nothing more we can do here except report the error
                if(secure) throw new NoSuchAlgorithmException("SecureRandom is broken. No other algorithms to try!");

                // Otherwise use non-secure
                RANDOM_SOURCE = new Random();

            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        */

        // Get array of random bytes
        byte[] random_array = new byte[length];
        RANDOM_SOURCE.nextBytes(random_array);
        return random_array;
    }

    /** Generates a random byte array. <B>NOTE:</B> This function is not guaranteed to draw from secure random source
     *  @param length length of byte array
     *  @return byte array of random bytes
     */
    public static byte[] generateRandomArray(final int length) {
        try{
            return generateRandomArray(length, false);
        }catch(NoSuchAlgorithmException nsae){
            nsae.printStackTrace(); // Will never be called
            throw new RuntimeException(nsae);
        }
    }


    /** Generates a random string
     *  @param length length of string
     *  @param secure true use only securerandom source, false to allow usage of non-secure random source if secure random
     *                source cannot be obtained
     *  @return string of random characters in hexidecimal format
     */
    public static String generateRandomString(final int length, final boolean secure) throws NoSuchAlgorithmException{
        // Convert to hex and return it
        return Utility.toHex(generateRandomArray((length/2) + (length % 2), secure)).substring(0, length);
    }

    /** Generates a random string. <B>NOTE:</B> This function is not guaranteed to draw from secure random source
     *  @param length length of string
     *  @return string of random characters in hexidecimal format
     */
    public static String generateRandomString(final int length){
        // Convert to hex and return it
        return Utility.toHex(generateRandomArray((length/2) + (length % 2))).substring(0, length);
    }

    public static <T> T checkValueForNull(final T value, final String variable_name){
    	if(value == null) throw new IllegalArgumentException(variable_name + " is null");
    	return value;
    }
}
