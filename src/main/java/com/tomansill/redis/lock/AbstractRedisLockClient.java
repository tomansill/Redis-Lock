package com.tomansill.redis.lock;

import com.tomansill.redis.exception.NoScriptFoundException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.Map;

/** AbstractRedisLockClient class
 *  This class is abstract and defines methods for subclasses to implement with their own Redis client.
 *  @author <a href="mailto:tom@ansill.com">Tom Ansill</a>
 */
public abstract class AbstractRedisLockClient{

    // ##### PUBLIC STATIC MEMBERS #####

    /** Default lease duration */
    public final static long DEFAULT_LEASE_DURATION_SECONDS = 60;

    // ##### PRIVATE STATIC MEMBERS #####

    /** Map of Redis lua scripts */
    private static Map<String, String> SCRIPT_NAME_TO_SCRIPTS = null;

    /** Map of Redis lua script hash */
    private static Map<String, String> SCRIPT_NAME_TO_SCRIPT_HASH = null;

    /* Map of lease durations per server */
    private final static Map<String,Duration> LEASE_DURATION = new HashMap<>(1);

    /* Lock for lease duration map */
    private final static ReentrantReadWriteLock LEASE_LOCK = new ReentrantReadWriteLock(true);

    /** Maximum retries number before quitting */
    private final static int MAX_RETRIES = 3;

    /* Comfy little class that holds time and time unit */
    private static class Duration{
        private long time;
        private TimeUnit unit;
        public Duration(final long time, final TimeUnit unit){
            this.time = time;
            this.unit = unit;
        }
        public synchronized long getTime(){
            return this.time;
        }
        public synchronized TimeUnit getUnit(){
            return this.unit;
        }
        public synchronized void set(final long time, final TimeUnit unit){
            this.time = time;
            this.unit = unit;
        }
    }

    /** Converts byte array to hex string
     *  @param array byte array
     *  @return hex string
     */
    private static String toHex(final byte[] array){
        char[] mapping = {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};
        char[] new_str = new char[array.length * 2];
        int map_index;
        for(int i = 0; i < array.length; i++){
            map_index = array[i] * 0xFF;
            new_str[i * 2] = mapping[map_index / 16];
            new_str[(i * 2) + 1] = mapping[map_index % 16];
        }
        return new String(new_str);
    }

    // ##### CLASS MEMBERS #####

    /** Client id to identify the client */
    private String client_id;

    /** Constructor for AbstractRedisLockClient
     *  On the first run, AbstractRedisLockClient will retrieve Redis Lua scripts
     *  that are located on the resources directory on the project and load them
     *  into the memory. Those scripts are reused on further constructor
     *  initializations.
     *  @throws ExceptionInInitializerError thrown if AbstractRedisLockClient has failed to retrieve scripts on the resources area
     */
    protected AbstractRedisLockClient() throws ExceptionInInitializerError{

        // Create unique ID //TODO ID too long?
        try{
            byte[] digest = new byte[32];
            SecureRandom sr = SecureRandom.getInstanceStrong();
            sr.nextBytes(digest);
            this.client_id = toHex(digest);
        }catch(NoSuchAlgorithmException nsae){
            // Compiler says unreachable here??? Disable this for now TODO
            //throw new ExceptionInInitializerError("Failed to initialize the client because SecureRandom does not have a suitable algorithm. Reason: " + nsae.getMessage());;
        }

        // Initialize scripts - done only once
        if(SCRIPT_NAME_TO_SCRIPTS == null && SCRIPT_NAME_TO_SCRIPT_HASH == null){

            // Initialize script maps
            SCRIPT_NAME_TO_SCRIPTS = new HashMap<>();
            SCRIPT_NAME_TO_SCRIPT_HASH = new HashMap<>();

            // Class loader
            ClassLoader class_loader = getClass().getClassLoader();

            // Filenames
            String[] filenames = {"single_write_lock", "single_write_unlock", "single_refire"};

            // Load scripts
            for(int i = 0; i < filenames.length; i++){

                // Get file
                File file = new File(class_loader.getResource(filenames[i] + ".lua").getFile());

                // Serious error occurs if those files cannot be found
                if(!file.exists()){
                    throw new ExceptionInInitializerError("Cannot find '" + filenames[i] + ".lua' on the package resouces directory!");
                }

                // Read the script
                StringBuilder sb = new StringBuilder();
                try(BufferedReader br = new BufferedReader(new FileReader(file))){
                    String line = null;
                    while((line = br.readLine()) != null) sb.append(line);
                }catch(IOException ioe){
                    throw new ExceptionInInitializerError("Failed to read '" + filenames[i] + ".lua' on the package resouces directory! Reason: " + ioe.getMessage());
                }

                // Add it to the script
                SCRIPT_NAME_TO_SCRIPTS.put(filenames[i], sb.toString());
                SCRIPT_NAME_TO_SCRIPT_HASH.put(filenames[i], null);
            }
        }
    }

    /** Returns host name and port in format of (hostname):(port)
     *  @return string format of (hostname):(port)
     */
    protected abstract String getHostnameAndPort();

    /** Returns true if this client is connected to a cluster, false otherwise
     *  @return true if this client is connected to a cluster, false otherwise
     */
    public abstract boolean isCluster();

    /** Retrieves current lease duration
     *  @param unit TimeUnit for returned time
     *  @return time in time unit provided in the parameter
     *  @throws IllegalArgumentException thrown if unit parameter is null
     */
    public long getLeaseDuration(final TimeUnit unit) throws IllegalArgumentException{

        // Check unit
        if(unit == null) throw new IllegalArgumentException("unit is null");

        // Result container
        long result = 0;

        // Get URL
        String url = this.getHostnameAndPort();

        // Obtain read lock and lock it
        Lock read_lock = LEASE_LOCK.readLock();
        read_lock.lock();

        // Retrieve time if it exists, otherwise use default one
        if(LEASE_DURATION.containsKey(url)){
            Duration duration = LEASE_DURATION.get(url);
            result = unit.convert(duration.getTime(), duration.getUnit());
        }else{
            result = unit.convert(DEFAULT_LEASE_DURATION_SECONDS, TimeUnit.SECONDS);
        }

        // Release lock
        read_lock.unlock();

        // Return it
        return result;
    }

    /** Sets lease duration
     *  @param time time
     *  @param unit TimeUnit
     *  @throws IllegalArgumentException thrown if unit parameter is null or time is a negative number
     */
    public void setLeaseDuration(final long time, final TimeUnit unit){

        // Check unit and time
        if(unit == null) throw new IllegalArgumentException("unit is null");
        if(time < 0) throw new IllegalArgumentException("time is not positive");

        // Get URL
        String url = this.getHostnameAndPort();

        // Obtain write lock and lock it
        Lock write_lock = LEASE_LOCK.writeLock();
        write_lock.lock();

        // Insert or update
        if(LEASE_DURATION.containsKey(url)){
            LEASE_DURATION.get(url).set(time, unit);
        }else{
            LEASE_DURATION.put(url, new Duration(time, unit));
        }

        // Release lock
        write_lock.unlock();
    }

    public RedisReadWriteLock getLock(final String lockpoint){
        return new RedisReadWriteLock(lockpoint, this);
    }

    /** Loads script on the server and retrieve SHA1 digest of script
     *  @param script Lua script
     *  @return SHA1 digest of script
     */
    protected abstract String scriptLoad(String script);

    /** Evaluates and returns boolean value
     *  @param hash hash to Lua script
     *  @param args argument parameters
     *  @return boolean
     *  @throws NoScriptFoundException thrown if the script to the corresponding hash cannot be found on the database
     */
    protected abstract boolean booleanEval(final String hash, final String... args) throws NoScriptFoundException;

    /** Evaluates and returns boolean value
     *  @param hash hash to Lua script
     *  @param args argument parameters
     *  @return string
     *  @throws NoScriptFoundException thrown if the script to the corresponding hash cannot be found on the database
     */
    protected abstract String stringEval(final String hash, final String... args) throws NoScriptFoundException;

    /** Performs a single write lock
     *  @param lockpoint lockpoint to acquire a lock
     *  @param lock_id id of lock
     *  @param is_fair true to enforce fairness policy, false otherwise
     *  @param first_time TODO
     *  @return true if lock was acquired, false otherwise
     */
    boolean singleWriteLock(final String lockpoint, final String lock_id, final boolean is_fair){
        return singleWriteLock(lockpoint, lock_id, is_fair, getLeaseDuration(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
    }

    /** Performs a single write lock
     *  @param lockpoint lockpoint to acquire a lock
     *  @param lock_id id of lock
     *  @param is_fair true to enforce fairness policy, false otherwise
     *  @return true if lock was acquired, false otherwise
     */
    boolean singleWriteLock(final String lockpoint, final String lock_id, final boolean is_fair, final long lock_lease_time, final TimeUnit unit){

        // Check if we have script loaded. If not, load it
        String script_name = "single_write_lock";
        if(!SCRIPT_NAME_TO_SCRIPT_HASH.containsKey(script_name)){
            SCRIPT_NAME_TO_SCRIPT_HASH.put(script_name, this.scriptLoad(SCRIPT_NAME_TO_SCRIPTS.get(script_name)));
        }

        // Get TimeUnit
        TimeUnit ms_unit = TimeUnit.MILLISECONDS;

        // Execute it
        try{
            return booleanEval(
                SCRIPT_NAME_TO_SCRIPT_HASH.get(script_name),
                lockpoint,
                lock_id,
                is_fair + "",
                true + "",
                ms_unit.convert(lock_lease_time, unit) + "",
                DEFAULT_LEASE_DURATION_SECONDS * 1000 + ""
            );

        // Catch a possible no script found error
        }catch(NoScriptFoundException nsfe){
            try{

                // Load the script
                SCRIPT_NAME_TO_SCRIPT_HASH.put(script_name, this.scriptLoad(SCRIPT_NAME_TO_SCRIPTS.get(script_name)));

                // Try again
                return booleanEval(
                    SCRIPT_NAME_TO_SCRIPT_HASH.get(script_name),
                    lockpoint,
                    lock_id,
                    is_fair + "",
                    true + "",
                    ms_unit.convert(lock_lease_time, unit) + "",
                    DEFAULT_LEASE_DURATION_SECONDS * 1000 + ""
                );

            }catch(NoScriptFoundException nsfe_again){
                // AbstractRedisLockClient is hosed at this point
                throw new RuntimeException(nsfe_again); //TODO different exception
            }
        }
    }

    /** Performs a single write unlock
     *  @param lockpoint lockpoint to acquire a lock
     *  @return true if lock was acquired, false otherwise
     */
    void singleWriteUnlock(final String lockpoint){

        // Check if we have script loaded. If not, load it
        String script_name = "single_write_unlock";
        if(!SCRIPT_NAME_TO_SCRIPT_HASH.containsKey(script_name)){
            SCRIPT_NAME_TO_SCRIPT_HASH.put(script_name, this.scriptLoad(SCRIPT_NAME_TO_SCRIPTS.get(script_name)));
        }

        // Execute it
        try{
            booleanEval(SCRIPT_NAME_TO_SCRIPT_HASH.get(script_name), lockpoint);

        // Catch a possible no script found error
        }catch(NoScriptFoundException nsfe){
            try{

                // Load the script
                SCRIPT_NAME_TO_SCRIPT_HASH.put(script_name, this.scriptLoad(SCRIPT_NAME_TO_SCRIPTS.get(script_name)));

                // Try again
                booleanEval(SCRIPT_NAME_TO_SCRIPT_HASH.get(script_name), lockpoint);

            }catch(NoScriptFoundException nsfe_again){
                // AbstractRedisLockClient is hosed at this point
                throw new RuntimeException(nsfe_again); //TODO different exception
            }
        }
    }
}
