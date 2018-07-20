package com.tomansill.redis.lock;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Random;

/** Sensitive data object, used for testing concurrency mechanisms
 *  @author <a href="mailto:tom@ansill.com">Tom Ansill</a>
 */
public class Utility{

    private Utility(){} // Prevents instantiation

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

    /** Generates a random string
     *  @param length
     *  @return
     */
    public static String generateRandomString(final int length){

        // Create digest array
        byte[] digest = new byte[length*2];

        // Use SecureRandom
        try {

            // Get a good instance
            SecureRandom sr = SecureRandom.getInstanceStrong();

            // Get digest
            sr.nextBytes(digest);

        }catch(NoSuchAlgorithmException nsae){

            // Less secure alternative - but hey, it's just for testing
            Random r = new Random();

            r.nextBytes(digest);
        }

        // Convert to hex and return it
        return Utility.toHex(digest);
    }
}
