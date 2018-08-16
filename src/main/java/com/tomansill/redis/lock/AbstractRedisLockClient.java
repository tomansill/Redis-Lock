package com.tomansill.redis.lock;

import com.tomansill.redis.exception.NoScriptFoundException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** AbstractRedisLockClient class
 *  This class is abstract and defines methods for subclasses to implement with their own Redis client.
 *  @author <a href="mailto:tom@ansill.com">Tom Ansill</a>
 */
public abstract class AbstractRedisLockClient{

    // ##### PUBLIC STATIC MEMBERS #####

    /** Default lease duration */
    private final static long DEFAULT_LEASE_DURATION_SECONDS = 60;

    // ##### PRIVATE STATIC MEMBERS #####

    /** Map of Redis lua scripts */
    private static Map<String, String> SCRIPT_NAME_TO_SCRIPTS = null;

    /** Map of Redis lua script hash */
    private static Map<String, String> SCRIPT_NAME_TO_SCRIPT_HASH = null;

    /** Map of lease durations per server */
    private final Duration lease_duration = new Duration(DEFAULT_LEASE_DURATION_SECONDS, TimeUnit.SECONDS);

    /** Comfy little class that holds time and time unit */
    private static class Duration{
        private long time;
        private TimeUnit unit;
        Duration(final long time, final TimeUnit unit){
            this.time = time;
            this.unit = unit;
        }
        synchronized TimeUnit getUnit(){
            return this.unit;
        }
        synchronized void set(final long time, final TimeUnit unit){
            this.time = time;
            this.unit = unit;
        }
        synchronized  long getTime(TimeUnit unit){
            return unit.convert(this.time, this.unit);
        }
    }

    // ##### CLASS MEMBERS #####

    /** Client id to identify the client */
    private final String client_id;

    /** Lock id to CDL Map */
    private final ConcurrentHashMap<String, CountDownLatch> lock_to_cdl_map = new ConcurrentHashMap<>();

    /** Set of unfair locks */
    private final Map<String,Set<String>> unfair_locks_set_map = new ConcurrentHashMap<>();

    /** Timers */
    private final Timer timer = new Timer();
    private final ConcurrentHashMap<String, TimerTask> lockpoint_to_timer = new ConcurrentHashMap<>();

    /** Prefix */
    private final String prefix;

    /** Constructor for AbstractRedisLockClient
     *  On the first run, AbstractRedisLockClient will retrieve Redis Lua scripts
     *  that are located on the resources directory on the project and load them
     *  into the memory. Those scripts are reused on further constructor
     *  initializations.
     *  @throws ExceptionInInitializerError thrown if AbstractRedisLockClient has failed to retrieve scripts on the
     *      resources area of Jar file
     */
    protected AbstractRedisLockClient(final String prefix) throws ExceptionInInitializerError{

    	if(prefix == null) this.prefix = "";
    	else this.prefix = prefix;

        // Get random string
        this.client_id = Utility.generateRandomString(8);

        System.out.println("Client id: " + this.client_id);

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
            for(String filename : filenames){

                // Get file
                File file = new File(class_loader.getResource(filename + ".lua").getFile());

                // Serious error occurs if those files cannot be found
                if(!file.exists()){
                    throw new ExceptionInInitializerError("Cannot find '" + filename + ".lua' on the package resouces directory!");
                }

                // Read the script
                StringBuilder sb = new StringBuilder();
                try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        int index = line.indexOf("--");
                        if(index != -1) line = line.substring(0, index);
                        if(!line.trim().equals("")) {
                            sb.append(line);
                            sb.append('\n');
                        }
                    }
                } catch (IOException ioe) {
                    throw new ExceptionInInitializerError("Failed to read '" + filename + ".lua' on the package resouces directory! Reason: " + ioe.getMessage());
                }

                // Add it to the script
                SCRIPT_NAME_TO_SCRIPTS.put(filename, sb.toString());
            }
        }
    }

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

        // Return it
        return lease_duration.getTime(unit);
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

        lease_duration.set(time, unit);
    }

    public RedisReadWriteLock getLock(final String lockpoint){
        return new RedisReadWriteLock(lockpoint, this);
    }

    public RedisReadWriteLock getLock(final String lockpoint, final boolean is_fair){
        return new RedisReadWriteLock(lockpoint, this, is_fair);
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

    /** Evaluates and returns string value
     *  @param hash hash to Lua script
     *  @param args argument parameters
     *  @return string
     *  @throws NoScriptFoundException thrown if the script to the corresponding hash cannot be found on the database
     */
    protected abstract String stringEval(final String hash, final String... args) throws NoScriptFoundException;

	/** Evaluates and returns long value
	 *  @param hash hash to Lua script
	 *  @param args argument parameters
	 *  @return string
	 *  @throws NoScriptFoundException thrown if the script to the corresponding hash cannot be found on the database
	 */
	protected abstract long longEval(final String hash, final String... args) throws NoScriptFoundException;

    /** Subscribes to channel
     *  @param channel channel name
     *  @param function function to fire when new topic comes up
     */
    protected abstract void subscribe(final String channel, final Consumer<String> function);

    /** Unsunscribes channel
     *  @param channel channel name
     */
    protected abstract void unsubscribe(final String channel);

    private CountDownLatch setUpSubscription(final String lockpoint, final String lock_id, final boolean is_fair, final boolean first_time){

        // Set up subscription listener
        if(first_time){

            // Set up subscription
            this.subscribe(lockpoint, this::processMessage);
        }

        // Subscribe
        CountDownLatch cdl = new CountDownLatch(1);
        this.lock_to_cdl_map.put(lock_id, cdl);
        if(!is_fair){
        	this.unfair_locks_set_map.putIfAbsent(lockpoint,new HashSet<>());
        	this.unfair_locks_set_map.get(lockpoint).add(lock_id);
        }
        return cdl;
    }

    private void tearDownSubscription(final String lockpoint, final String lock_id){

        // Remove entry
        this.lock_to_cdl_map.remove(lock_id);
        synchronized(this.unfair_locks_set_map) {
            this.unfair_locks_set_map.remove(lock_id);
        }

        // Unsubscribe
        this.unsubscribe(lockpoint);
    }

    private boolean performBooleanEval(final String script_name, final String... arguments){

        // Check if we have script loaded. If not, load it on the database
        if(!SCRIPT_NAME_TO_SCRIPT_HASH.containsKey(script_name)){
            SCRIPT_NAME_TO_SCRIPT_HASH.put(script_name, this.scriptLoad(SCRIPT_NAME_TO_SCRIPTS.get(script_name)));
        }

        try{
            return this.booleanEval(SCRIPT_NAME_TO_SCRIPT_HASH.get(script_name), arguments);
        }catch(NoScriptFoundException nsfe){ // Catch a possible no script found error
            try{

                // Load the script
                SCRIPT_NAME_TO_SCRIPT_HASH.put(script_name, this.scriptLoad(SCRIPT_NAME_TO_SCRIPTS.get(script_name)));

                // Try again
                return this.booleanEval(SCRIPT_NAME_TO_SCRIPT_HASH.get(script_name), arguments);

            }catch(NoScriptFoundException nsfe_again){
                // AbstractRedisLockClient is hosed at this point
                throw new RuntimeException(nsfe_again); //TODO different exception
            }
        }
    }

	private long performLongEval(final String script_name, final String... arguments){

		// Check if we have script loaded. If not, load it on the database
		if(!SCRIPT_NAME_TO_SCRIPT_HASH.containsKey(script_name)){
			SCRIPT_NAME_TO_SCRIPT_HASH.put(script_name, this.scriptLoad(SCRIPT_NAME_TO_SCRIPTS.get(script_name)));
		}

		try{
			return this.longEval(SCRIPT_NAME_TO_SCRIPT_HASH.get(script_name), arguments);
		}catch(NoScriptFoundException nsfe){ // Catch a possible no script found error
			try{

				// Load the script
				SCRIPT_NAME_TO_SCRIPT_HASH.put(script_name, this.scriptLoad(SCRIPT_NAME_TO_SCRIPTS.get(script_name)));

				// Try again
				return this.longEval(SCRIPT_NAME_TO_SCRIPT_HASH.get(script_name), arguments);

			}catch(NoScriptFoundException nsfe_again){
				// AbstractRedisLockClient is hosed at this point
				throw new RuntimeException(nsfe_again); //TODO different exception
			}
		}
	}

    private boolean performSingleMasterLock(
    		final String lockpoint,
		    final String lock_id,
		    final boolean is_read_lock,
		    final boolean try_lock,
		    final boolean is_fair,
		    final boolean first_attempt,
		    final TimeUnit unit,
		    final long lock_lease_time
    ){

        // Get TimeUnit
        TimeUnit ms_unit = TimeUnit.MILLISECONDS;

        // Evaluate
        long duration = this.performLongEval(
            "single_write_lock",
            lockpoint,
            this.client_id,
            lock_id,
	        is_read_lock ? "1" : "0",
            is_fair ? "1" : "0",
            first_attempt ? "1" : "0",
            ms_unit.convert(lock_lease_time, unit) + "",
            DEFAULT_LEASE_DURATION_SECONDS * 1000 + "",
	        prefix,
	        try_lock ? "1" : "0"
        );

        // If equals to zero, means lock is successful
        if(duration == 0) return true;

        // Set timer if more than zero
        if(duration > 0) this.setRefireTimer(lockpoint, duration);

        // Report if some weird number is returned
	    if(duration < -1) System.err.println("Weird number while performing lock. Number: " + duration);

        return false;
    }

    private void performSingleWriteUnlock(final String lockpoint){

        // Evaluate
        this.performBooleanEval(
            "single_write_unlock",
            lockpoint
        );
    }

    /** Performs a single write lock
     *  @param lockpoint lockpoint to acquire a lock
     *  @param lock_id id of lock
     *  @param is_fair true to enforce fairness policy, false otherwise
     *  @return true if lock was acquired, false otherwise
     */
    boolean performLock(final String lockpoint, final String lock_id, final boolean is_read_lock, final boolean try_lock, final boolean is_fair, final long time_out, final TimeUnit unit) throws InterruptedException{
	    return performLock(lockpoint, lock_id, is_read_lock, try_lock, is_fair, time_out, unit, getLeaseDuration(unit));
    }

    /** Performs a single write lock
     *  @param lockpoint lockpoint to acquire a lock
     *  @param lock_id id of lock
     *  @param is_fair true to enforce fairness policy, false otherwise
     *  @return true if lock was acquired, false otherwise
     */
    boolean performLock(final String lockpoint, final String lock_id, final boolean is_read_lock, final boolean try_lock, final boolean is_fair, final long time_out, final TimeUnit unit, final long lock_lease_time) throws InterruptedException{

        //System.out.println("performLock(lockpoint=" + lockpoint + ", lock_id=" + lock_id + ", is_fair=" + is_fair + ", time_out=" + time_out + ", unit=" + unit + " lock_lease_time=" + lock_lease_time + ")");

        long actual_lease_time = (lock_lease_time < 1 ? getLeaseDuration(unit): lock_lease_time);
        boolean result;
        boolean first_attempt = true;
        long start_time = System.currentTimeMillis(); // Record the start time before continuing
        try{
            do{
                // Subscribe now (except for trylocks)
                /*  REASON WHY WE DO THIS NOW INSTEAD OF ON UNSUCCESSFUL LOCK:
                 *  Suppose we attempt to lock but it is unavailable, as soon as the locking script exits,
                 *  other lock may unlock the lockpoint then announce and exit the unlocking script.
                 *  Then this lock will fire the subscription but it had already missed the announcement
                 *  and get stuck on waiting for lock message that may never arrive.
                 */
                CountDownLatch cdl = null;
	            if(!try_lock) cdl = this.setUpSubscription(lockpoint, lock_id, is_fair, first_attempt);

                // Execute it
                result = this.performSingleMasterLock(lockpoint, lock_id, is_read_lock, try_lock, is_fair, first_attempt, unit, actual_lease_time);
                first_attempt = false;

                //System.out.println("lock id: " + lock_id + " result: " + result);

                // If it was not success, wait then try again
                if(!result && !try_lock){

                    System.out.println(this.client_id + " #lock id: " + lock_id + " awaits.");

                    // Wait for next unlock
                    boolean await_result = true;
                    if (time_out < 0) cdl.await();
                    else{
                        long new_time = unit.convert((TimeUnit.MILLISECONDS.convert(time_out, unit) - (System.currentTimeMillis() - start_time)), TimeUnit.MILLISECONDS);
                        await_result = cdl.await(new_time, unit);
                    }

                    System.out.println(this.client_id + " #lock id: " + lock_id + " await_result: " + await_result);

                    // Check
                    if(!await_result) return false; // Give up

                // Successful
                }else return result;
            }while(true);
        }finally{
            // Tear down
            this.tearDownSubscription(lockpoint, lock_id);
        }
    }

    /** Performs a single write unlock
     *  @param lockpoint lockpoint to unlock
     */
    void writeUnlock(final String lockpoint, final String lock_id){

        //System.out.println("writeUnlock(lockpoint=" + lockpoint + ", lock_id=" + lock_id +")");

        System.out.println(this.client_id + " lock_id: " + lock_id);

        this.performSingleWriteUnlock(lockpoint);
    }

    private void processMessage(final String message){

        // Check if it's unfair unlock message
        if(message.charAt(0) == '#') { // Unfair

            // "Randomly" choose an element
            String lock_id = null;
            String lockpoint = message.substring(1);
            synchronized(this.unfair_locks_set_map) {
                if(this.unfair_locks_set_map.get(lockpoint).iterator().hasNext()) {
                    lock_id = this.unfair_locks_set_map.get(lockpoint).iterator().next();
                }
            }

            // Check if the lock actually exists
            if(lock_id == null) return;

            // Find CDL if there's any and fire it
            CountDownLatch cdl = this.lock_to_cdl_map.get(lock_id);
            if(cdl != null) cdl.countDown();
            else {
                // TODO run refire function
                System.err.println("panic 2");
            }

        } else { // Possibly fair

            // Read the message for the delimiters - find client index and lock index
            // Message is in format of "<event_type>:<client_id>:<lock_id>:<lockpoint>"
            int client_index = message.indexOf(":");
            int lock_index = message.indexOf(":", client_index + 1);
            int lockpoint_index = message.indexOf(":", lock_index + 1);

            // If any of indices are -1, then no ":" are found so the message is invalid
            if(client_index == -1 || lock_index == -1) return;

            // Extract event type
            String event_type = message.substring(0, client_index);

            // Check event type
            if(event_type.equals("l")){ // locked event
                // TODO
            }else if(event_type.equals("o")){ // Unlock event

                //Extract client id
                String client_id = message.substring(client_index + 1, lock_index);

                // Check if client id matches
                if(!this.client_id.equals(client_id)) { // Doesn't match
                    // TODO set timer
                    //System.err.println("panic 3");
                    return;
                }

                // Extract lock id
                String lock_id = message.substring(lock_index + 1, lockpoint_index);

                // Find the matching lock and count it down
                CountDownLatch cdl = this.lock_to_cdl_map.remove(lock_id);
                if(cdl != null) cdl.countDown();
                else this.refire(message.substring(lockpoint_index + 1));
            }
        }
    }

    private void setRefireTimer(final String lockpoint, long duration){

    	// Check if timer is set to indefinite
    	if(duration == -1) duration = DEFAULT_LEASE_DURATION_SECONDS * 1000;

	    // New Timertask
	    TimerTask new_timer_task = new TimerTask(){
		    @Override
		    public void run(){
		    	lockpoint_to_timer.remove(lockpoint);
			    refire(lockpoint);
		    }
	    };

	    // Put it in the map
	    TimerTask ret_task = this.lockpoint_to_timer.put(lockpoint, new_timer_task);

	    // Cancel previous timer task
	    if(ret_task != null){
	    	ret_task.cancel();
	    }

	    // Start timer
	    this.timer.schedule(new_timer_task, duration);
    }

    private void refire(final String lockpoint){

    	long duration = this.performLongEval(
            "single_instance_refire",
		    lockpoint,
		    DEFAULT_LEASE_DURATION_SECONDS * 1000 + "",
		    prefix
	    );

    	// If equals zero, then go ahead and launch another timertask to perform a very short wait
    	if(duration == 0) setRefireTimer(lockpoint, (DEFAULT_LEASE_DURATION_SECONDS * 1000)/4);

    	// If equals to -1, means no expiration time on that lock, so refire to poll that lock
    	else if(duration <= -1) setRefireTimer(lockpoint, DEFAULT_LEASE_DURATION_SECONDS * 1000);

    	// Else, wait for that duration
	    else setRefireTimer(lockpoint, duration);
    }

    private static class Message{

    	/** enum **/
	    private enum Type{
    		LOCK,
		    UNLOCK,
		    SHARED,
		    OPEN,
		    FREE
	    }

	    public final Type type;

	    public final String client_id;

	    public final String lock_id;

	    public final String lockpoint;

	    public final long lease_time;

	    public Message(final Type type, final String client_id, final String lock_id, final String lockpoint, final long lease_time){
	    	this.type = type;
	    	this.client_id = client_id;
	    	this.lock_id = lock_id;
	    	this.lockpoint = lockpoint;
	    	this.lease_time = lease_time;
	    }

	    public static Message interpret(final String message){
	    	if(message == null || message.isEmpty()) return null;

	    	try{
			    // Get type
			    Type type;
			    if(message.charAt(0) == 'l') type = Type.LOCK;
			    else if(message.charAt(0) == 'u') type = Type.UNLOCK;
			    else if(message.charAt(0) == 's') type = Type.SHARED;
			    else if(message.charAt(0) == 'o') type = Type.OPEN;
			    else if(message.charAt(0) == '#') type = Type.FREE;
			    else return null;

			    // Check the semicolon after type
			    if(message.charAt(1) != ':') return null;

			    // If Free or Shared message, then scan all and return message
			    if(type == Type.FREE || type == Type.SHARED){
				    return new Message(type, null, null, message.substring(3), 0);
			    }

			    // Get first semicolon
			    int first_semi = message.indexOf(":", 2);

			    // Client id
			    String client_id = message.substring(3, first_semi);

			    // Get second semicolon
			    int second_semi = message.indexOf(":", first_semi + 1);

			    // lock id
			    String lock_id = message.substring(first_semi + 1, second_semi);

			    return null;
		    }catch(IndexOutOfBoundsException ignored){
	    		return null;
		    }
	    }
    }
}
