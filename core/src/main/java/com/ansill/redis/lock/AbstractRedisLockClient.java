package com.ansill.redis.lock;

import com.ansill.redis.lock.exception.NoScriptFoundException;
import com.ansill.redis.lock.exception.ScriptErrorException;
import com.ansill.redis.lock.exception.ScriptHashErrorException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * AbstractRedisLockClient class
 * This class is abstract and defines methods for subclasses to implement with their own Redis client.
 *
 * @author <a href="mailto:tom@ansill.com">Tom Ansill</a>
 */
public abstract class AbstractRedisLockClient{

    // ##### PUBLIC STATIC MEMBERS #####

    /**
     * Default lease duration
     */
    private final static long DEFAULT_LEASE_DURATION_SECONDS = 60;

    /**
     * Debug flag
     */
    private static boolean DEBUG = false;

    /**
     * Debug id
     */
    private static String DEBUG_ID = Utility.generateRandomString(8);

    // ##### PRIVATE STATIC MEMBERS #####
    /**
     * Map of Redis lua scripts
     */
    private static Map<String, String> SCRIPT_NAME_TO_SCRIPTS = null;
    /**
     * Map of Redis lua script name to hash
     */
    private static Map<String, String> SCRIPT_NAME_TO_SCRIPT_HASH = null;
    /**
     * Map of Redis lua script hash to name
     */
    private static Map<String, String> SCRIPT_HASH_TO_SCRIPT_NAME = null;
    /**
     * Map of lease durations per server
     */
    private final Duration lease_duration = new Duration(DEFAULT_LEASE_DURATION_SECONDS, TimeUnit.SECONDS);
    /**
     * Client id to identify the client
     */
    private final String client_id;
    /**
     * Lock id to CDL Map
     */
    private final ConcurrentHashMap<String, CountDownLatch> lock_to_cdl_map = new ConcurrentHashMap<>();

    // ##### CLASS MEMBERS #####
    /**
     * Set of unfair locks
     */
    private final Map<String, Set<String>> unfair_locks_set_map = new ConcurrentHashMap<>();
    /**
     * Set of shared locks
     */
    private final Map<String, Set<String>> shared_locks_set_map = new ConcurrentHashMap<>();
    /**
     * Set of shared lock owners
     */
    private final Set<String> shared_lock_owners = new HashSet<>();
    /**
     * Timers
     */
    private final Timer timer = new Timer();
    private final ConcurrentHashMap<String, TimerTask> lockpoint_to_timer = new ConcurrentHashMap<>();

    /**
     * Prefix
     */
    private final String prefix;

    /**
     * Debug
     */
    private final boolean debug = true;
    private boolean listening = false;

    /**
     * Constructor for AbstractRedisLockClient
     * On the first run, AbstractRedisLockClient will retrieve Redis Lua scripts
     * that are located on the resources directory on the project and load them
     * into the memory. Those scripts are reused on further constructor
     * initializations.
     *
     * @throws ExceptionInInitializerError thrown if AbstractRedisLockClient has failed to retrieve scripts on the
     *                                     resources area of Jar file
     */
    protected AbstractRedisLockClient(@Nullable String prefix) throws ExceptionInInitializerError {

        if (prefix == null) this.prefix = "";
        else this.prefix = prefix;

        // Setup
        setUp(debug);

        // Get random string
        this.client_id = Utility.generateRandomString(8);
    }

    public static void setUp(boolean debug) {

        // Initialize scripts - done only once
        if ((SCRIPT_NAME_TO_SCRIPTS == null && SCRIPT_NAME_TO_SCRIPT_HASH == null && SCRIPT_HASH_TO_SCRIPT_NAME == null) || !(DEBUG == debug)) {

            // Update
            DEBUG = debug;

            // Initialize script maps
            SCRIPT_NAME_TO_SCRIPTS = new HashMap<>();
            SCRIPT_NAME_TO_SCRIPT_HASH = new HashMap<>();
            SCRIPT_HASH_TO_SCRIPT_NAME = new HashMap<>();

            // Class loader
            ClassLoader class_loader = AbstractRedisLockClient.class.getClassLoader();

            // Filenames
            String[] filenames = {"single_instance_lock", "single_instance_refire", "single_instance_unlock"};

            // Load scripts
            for (String filename : filenames) {

                // Get file
                URL resource = class_loader.getResource(filename + ".lua");
                if (resource == null || resource.getFile() == null) {
                    throw new ExceptionInInitializerError("Cannot find '" + filename + ".lua' on the package resouces directory!");
                }
                File file = new File(resource.getFile());

                // Serious error occurs if those files cannot be found
                if (!file.exists()) {
                    throw new ExceptionInInitializerError("Cannot find '" + filename + ".lua' on the package resouces directory!");
                }

                // Read the script
                StringBuilder sb = new StringBuilder();
                try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                    //System.out.println(file);
                    String line;
                    boolean debug_block = false;
                    while ((line = br.readLine()) != null) {

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
                                line = line.replace("debug_print(", "redis.call(\"PUBLISH\", \"" + DEBUG_ID + "_DEBUG\", ")
                                        .trim();
                            }

                            // Check if it's worth adding into output script
                            if (!line.trim().equals("")) {
                                sb.append(line);
                                sb.append('\n');
                            }
                        }
                    }

                    // Catch trailing debug block
                    if (debug_block) throw new RuntimeException("Trailing --!start debug block!");
                } catch (IOException ioe) {
                    throw new ExceptionInInitializerError("Failed to read '" + filename + ".lua' on the package resouces directory! Reason: " + ioe
                            .getMessage());
                }

                //System.out.println("script: \n" + sb.toString());

                // Add it to the script
                SCRIPT_NAME_TO_SCRIPTS.put(filename, sb.toString());
            }
        }
    }

    private void setUpDebug() {

        // If debug is enabled, set up subscription to listen for debug messages
        if (debug && !listening) {
            listening = true;
            this.subscribe(DEBUG_ID + "_DEBUG", (message) -> System.out.println("DEBUG: " + message.replace("\\\t", "\t").replace("\\n", "\n")));
            Runtime.getRuntime().addShutdownHook(new Thread(() -> unsubscribe(DEBUG_ID + "_DEBUG")));
        }
    }

    /**
     * Returns true if this client is connected to a cluster, false otherwise
     *
     * @return true if this client is connected to a cluster, false otherwise
     */
    public abstract boolean isCluster();

    /**
     * Retrieves current lease duration
     *
     * @param unit TimeUnit for returned time
     * @return time in time unit provided in the parameter
     * @throws IllegalArgumentException thrown if unit parameter is null
     */
    public long getLeaseDuration(@Nonnull TimeUnit unit) throws IllegalArgumentException {

        // Return it
        return this.lease_duration.getTime(unit);
    }

    /**
     * Sets lease duration
     *
     * @param time time
     * @param unit TimeUnit
     * @throws IllegalArgumentException thrown if unit parameter is null or time is a negative number
     */
    public void setLeaseDuration(@Nonnegative long time, @Nonnull TimeUnit unit) {
        this.lease_duration.set(time, unit);
    }

    @Nonnull
    public RedisReadWriteLock getLock(@Nonnull String lockpoint) {
        return new RedisReadWriteLock(lockpoint, this);
    }

    @Nonnull
    public RedisReadWriteLock getLock(@Nonnull String lockpoint, boolean is_fair) {
        return new RedisReadWriteLock(lockpoint, this, is_fair);
    }

    @Nonnull
    private String innerScriptLoad(@Nonnull String script) {
        setUpDebug();
        try {
            return scriptLoad(script);
        } catch (ScriptHashErrorException see) {
            throw new ScriptErrorException("", script, see);
        } catch (NoScriptFoundException e) {
            throw new RuntimeException(e); // Will never be thrown
        }
    }

    /**
     * Loads script on the server and retrieve SHA1 digest of script
     *
     * @param script Lua script
     * @return SHA1 digest of script
     */
    @Nonnull
    protected abstract String scriptLoad(@Nonnull String script) throws ScriptHashErrorException, NoScriptFoundException;

    /**
     * Evaluates and returns boolean value
     *
     * @param hash hash to Lua script
     * @param args argument parameters
     * @return boolean
     * @throws NoScriptFoundException thrown if the script to the corresponding hash cannot be found on the database
     */
    protected abstract boolean booleanEval(@Nonnull String hash, @Nonnull String... args) throws NoScriptFoundException,
            ScriptHashErrorException;

    /**
     * Evaluates and returns string value
     *
     * @param hash hash to Lua script
     * @param args argument parameters
     * @return string
     * @throws NoScriptFoundException thrown if the script to the corresponding hash cannot be found on the database
     */
    @Nullable
    protected abstract String stringEval(@Nonnull String hash, @Nonnull String... args) throws NoScriptFoundException,
            ScriptHashErrorException;

    /**
     * Evaluates and returns long value
     *
     * @param hash hash to Lua script
     * @param args argument parameters
     * @return string
     * @throws NoScriptFoundException thrown if the script to the corresponding hash cannot be found on the database
     */
    protected abstract long longEval(@Nonnull String hash, @Nonnull String... args) throws NoScriptFoundException,
            ScriptHashErrorException;

    /**
     * Subscribes to channel
     *
     * @param channel  channel name
     * @param function function to fire when new topic comes up
     */
    public abstract void subscribe(@Nonnull String channel, @Nonnull Consumer<String> function);

    /**
     * Unsunscribes channel
     *
     * @param channel channel name
     */
    public abstract void unsubscribe(@Nonnull String channel);

    @Nonnull
    private CountDownLatch setUpSubscription(
            @Nonnull String lockpoint,
            @Nonnull String lock_id,
            boolean is_read_lock,
            boolean is_fair,
            boolean first_time
    ) {

        System.out.println("setUpSubscription " + lockpoint);

        // Set up subscription listener
        if (first_time) {

            // Set up subscription
            this.subscribe(prefix + "lockchannel:" + lockpoint, this::processMessage);
        }

        // Subscribe
        CountDownLatch cdl = new CountDownLatch(1);
        this.lock_to_cdl_map.put(lock_id, cdl);
        if (!is_fair) {
            synchronized (this.unfair_locks_set_map) {
                this.unfair_locks_set_map.putIfAbsent(lockpoint, new HashSet<>());
                this.unfair_locks_set_map.get(lockpoint).add(lock_id);
            }
        }
        if (is_read_lock) {
            synchronized (this.shared_locks_set_map) {
                this.shared_locks_set_map.putIfAbsent(lockpoint, new HashSet<>());
                this.shared_locks_set_map.get(lockpoint).add(lock_id);
            }
        }
        return cdl;
    }

    private void tearDownSubscription(@Nonnull String lockpoint, @Nonnull String lock_id) {

        System.out.println("tearDownSubscription lp:" + lockpoint + " id:" + lock_id);

        // Remove entry
        this.lock_to_cdl_map.remove(lock_id);
        synchronized (this.unfair_locks_set_map) {
            Set<String> locks = this.unfair_locks_set_map.get(lockpoint);
            if (locks != null) {
                locks.remove(lock_id);
                if (locks.isEmpty()) this.unfair_locks_set_map.remove(lockpoint);
            }
        }
        synchronized (this.shared_locks_set_map) {
            Set<String> locks = this.shared_locks_set_map.get(lockpoint);
            if (locks != null) {
                locks.remove(lock_id);
                if (locks.isEmpty()) this.shared_locks_set_map.remove(lockpoint);
            }
        }

        // Unsubscribe
        this.unsubscribe(lockpoint);
    }

    private boolean performBooleanEval(@Nonnull String script_name, @Nonnull String... arguments) {

        // Check if we have script loaded. If not, load it on the database
        if (!SCRIPT_NAME_TO_SCRIPT_HASH.containsKey(script_name)) {
            String script_hash = this.innerScriptLoad(SCRIPT_NAME_TO_SCRIPTS.get(script_name));
            SCRIPT_NAME_TO_SCRIPT_HASH.put(script_name, script_hash);
            SCRIPT_HASH_TO_SCRIPT_NAME.put(script_hash, script_name);
        }

        try {
            try {
                return this.booleanEval(SCRIPT_NAME_TO_SCRIPT_HASH.get(script_name), arguments);
            } catch (NoScriptFoundException nsfe) { // Catch a possible no script found error
                try {

                    // Load the script
                    SCRIPT_NAME_TO_SCRIPT_HASH.put(
                            script_name,
                            this.innerScriptLoad(SCRIPT_NAME_TO_SCRIPTS.get(script_name))
                    );

                    // Try again
                    return this.booleanEval(SCRIPT_NAME_TO_SCRIPT_HASH.get(script_name), arguments);

                } catch (NoScriptFoundException nsfe_again) {
                    // AbstractRedisLockClient is hosed at this point
                    throw new RuntimeException(nsfe_again); //TODO different exception
                }
            }
        } catch (ScriptHashErrorException see) {
            String name = SCRIPT_HASH_TO_SCRIPT_NAME.get(see.getScriptHash());
            String script = SCRIPT_NAME_TO_SCRIPTS.get(name);
            throw new ScriptErrorException(name, script, see);
        }
    }

    private long performLongEval(@Nonnull String script_name, @Nonnull String... arguments) {

        // Check if we have script loaded. If not, load it on the database
        if (!SCRIPT_NAME_TO_SCRIPT_HASH.containsKey(script_name)) {
            String script_hash = this.innerScriptLoad(SCRIPT_NAME_TO_SCRIPTS.get(script_name));
            SCRIPT_NAME_TO_SCRIPT_HASH.put(script_name, script_hash);
            SCRIPT_HASH_TO_SCRIPT_NAME.put(script_hash, script_name);
        }

        try {
            try {
                return this.longEval(SCRIPT_NAME_TO_SCRIPT_HASH.get(script_name), arguments);
            } catch (NoScriptFoundException nsfe) { // Catch a possible no script found error

                try {

                    // Load the script
                    SCRIPT_NAME_TO_SCRIPT_HASH.put(
                            script_name,
                            this.innerScriptLoad(SCRIPT_NAME_TO_SCRIPTS.get(script_name))
                    );

                    // Try again
                    return this.longEval(SCRIPT_NAME_TO_SCRIPT_HASH.get(script_name), arguments);

                } catch (NoScriptFoundException nsfe_again) {
                    // AbstractRedisLockClient is hosed at this point
                    throw new RuntimeException(nsfe_again); //TODO different exception
                }
            }
        } catch (ScriptHashErrorException see) {
            String name = SCRIPT_HASH_TO_SCRIPT_NAME.get(see.getScriptHash());
            String script = SCRIPT_NAME_TO_SCRIPTS.get(name);
            throw new ScriptErrorException(name, script, see);
        }
    }

    private boolean performSingleMasterLock(
            @Nonnull String lockpoint,
            @Nonnull String lock_id,
            boolean is_read_lock,
            boolean try_lock,
            boolean is_fair,
            boolean first_attempt,
            @Nonnull TimeUnit unit,
            long lock_lease_time
    ) {

		/*
		System.out.println(
				"performSingleMasterLock(lockpoint=" + lockpoint
				+ "\t lock_id=" + lock_id
				+ "\t is_read_lock=" + is_read_lock
				+ "\t try_lock=" + try_lock
				+ "\t is_fair=" + is_fair
				+ "\t first_attempt=" + first_attempt
				+ "\t unit=" + unit
				+ "\t lock_lease_time=" + lock_lease_time
				+ ")"
		);*/

        // Get TimeUnit
        TimeUnit ms_unit = TimeUnit.MILLISECONDS;

        // Evaluate
        long duration = this.performLongEval(
                "single_instance_lock",
                lockpoint,
                this.client_id,
                lock_id,
                is_fair ? "1" : "0",
                first_attempt ? "1" : "0",
                ms_unit.convert(lock_lease_time, unit) + "",
                DEFAULT_LEASE_DURATION_SECONDS * 1000 + "",
                is_read_lock ? "1" : "0",
                prefix,
                try_lock ? "1" : "0"
        );

        //System.out.println("lock_id: " + lock_id + " result: " + duration);

        // If equals to zero, means lock is successful
        if (duration == 0) {

            // If readlock, indicate as a lockowner
            if (is_read_lock) {
                synchronized (this.shared_lock_owners) {
                    this.shared_lock_owners.add(lock_id);
                }
            }

            return true;
        }

        // If equals to -3, means shared lock
        if (duration == -3) return true;

        // Set timer if more than zero
        if (duration > 0) this.setRefireTimer(lockpoint, duration);

        // Report if some weird number is returned
        if (duration < -1) System.err.println("Weird number while performing lock. Number: " + duration);

        return false;
    }

    private void performSingleMasterUnlock(@Nonnull String lockpoint, boolean is_read_lock, boolean is_owner) {

        // Evaluate
        this.performBooleanEval(
                "single_instance_unlock",
                lockpoint,
                is_read_lock ? "1" : "0",
                prefix,
                is_owner ? "1" : "0"
        );
    }

    /**
     * Performs a single write lock
     *
     * @param lockpoint lockpoint to acquire a lock
     * @param lock_id   id of lock
     * @param is_fair   true to enforce fairness policy, false otherwise
     * @return true if lock was acquired, false otherwise
     */
    boolean performLock(
            @Nonnull String lockpoint,
            @Nonnull String lock_id,
            boolean is_read_lock,
            boolean try_lock,
            boolean is_fair,
            long time_out,
            TimeUnit unit
    ) throws InterruptedException {
        TimeUnit tu = unit == null ? TimeUnit.MILLISECONDS : unit;
        return performLock(lockpoint, lock_id, is_read_lock, try_lock, is_fair, time_out, tu, getLeaseDuration(tu));
    }

    /**
     * Performs a single write lock
     *
     * @param lockpoint lockpoint to acquire a lock
     * @param lock_id   id of lock
     * @param is_fair   true to enforce fairness policy, false otherwise
     * @return true if lock was acquired, false otherwise
     */
    boolean performLock(
            @Nonnull String lockpoint,
            @Nonnull String lock_id,
            boolean is_read_lock,
            boolean try_lock,
            boolean is_fair,
            long time_out,
            @Nullable TimeUnit unit,
            long lock_lease_time
    ) throws InterruptedException {

        //System.out.println("performLock(lockpoint=" + lockpoint + ", lock_id=" + lock_id + ", is_read_lock=" + is_read_lock + ", is_fair=" + is_fair + ", time_out=" + time_out + ", unit=" + unit + " lock_lease_time=" + lock_lease_time + ", try_lock=" + try_lock + ")");

        long actual_lease_time = (lock_lease_time < 1 ? getLeaseDuration(unit) : lock_lease_time);
        boolean result;
        boolean first_attempt = true;
        long start_time = System.currentTimeMillis(); // Record the start time before continuing
        try {
            do {
                // Subscribe now (except for instant trylocks)
                /*  REASON WHY WE DO THIS NOW INSTEAD OF ON UNSUCCESSFUL LOCK:
                 *  Suppose we attempt to lock but it is unavailable, as soon as the locking script exits,
                 *  other lock may unlock the lockpoint then announce and exit the unlocking script.
                 *  Then this lock will fire the subscription but it had already missed the announcement
                 *  and get stuck on waiting for lock message that may never arrive.
                 */
                CountDownLatch cdl = null;
                if (time_out != 0)
                    cdl = this.setUpSubscription(lockpoint, lock_id, is_read_lock, is_fair, first_attempt);

                // Execute it
                result = this.performSingleMasterLock(
                        lockpoint,
                        lock_id,
                        is_read_lock,
                        (time_out == 0),
                        is_fair,
                        first_attempt,
                        unit,
                        actual_lease_time
                );
                first_attempt = false;

                //System.out.println("client_id: " + client_id + " lock id: " + lock_id + " result: " + result);

                // If it was not success, wait then try again
                if (!result && (time_out != 0)) {

                    System.out.println(this.client_id + " #lock id: " + lock_id + " awaits.");

                    // Wait for next unlock
                    boolean await_result = true;
                    if (time_out < 0) cdl.await();
                    else {
                        long new_time = unit.convert((TimeUnit.MILLISECONDS.convert(
                                time_out,
                                unit
                        ) - (System.currentTimeMillis() - start_time)), TimeUnit.MILLISECONDS);
                        await_result = cdl.await(new_time, unit);
                    }

                    System.out.println(this.client_id + " #lock id: " + lock_id + " await_result: " + await_result);

                    // Check
                    if (!await_result) return false; // Give up

                    // Successful
                } else return result;
            } while (true);
        } finally {
            // Tear down
            this.tearDownSubscription(lockpoint, lock_id);
            //System.out.println("lock_id " + lock_id + " has exited.");
        }
    }

    /**
     * Performs a single write unlock
     *
     * @param lockpoint lockpoint to unlock
     */
    void unlock(@Nonnull String lockpoint, @Nonnull String lock_id, boolean is_read_lock) {

        System.out.println("unlock(lockpoint=" + lockpoint + ", lock_id=" + lock_id + ")");

        // Check if lock owner
        boolean lock_owner;
        synchronized (this.shared_lock_owners) {
            lock_owner = shared_lock_owners.remove(lock_id);
        }

        // Call it
        this.performSingleMasterUnlock(lockpoint, is_read_lock, lock_owner);
    }

    private void processMessage(@Nonnull String message) {

        //System.out.println("processMessage(message=" + message + ")");

        Optional<Message> o_notification_message = Message.interpret(message);

        if(!o_notification_message.isPresent()) return;

        Message notification_message = o_notification_message.get();

        System.out.println("processMessage - message: " + notification_message);

        if(notification_message.getType() == Message.Type.FREE){

            System.out.println("lockpoint: " + notification_message.getLockpoint());
            System.out.println("map: " + unfair_locks_set_map);

            // "Randomly" choose an element
            String random_lock_id = null;
            synchronized (this.unfair_locks_set_map) {
                if (
                        this.unfair_locks_set_map.containsKey(notification_message.getLockpoint()) &&
                        this.unfair_locks_set_map.get(notification_message.getLockpoint()).iterator().hasNext()
                ) {
                    random_lock_id = this.unfair_locks_set_map.get(notification_message.getLockpoint())
                                                              .iterator()
                                                              .next();
                }
            }

            // Check if the lock actually exists
            if (random_lock_id == null) return;

            // Find CDL if there's any and fire it
            CountDownLatch cdl = this.lock_to_cdl_map.get(random_lock_id);
            if (cdl != null) cdl.countDown();
            else {
                // TODO run refire function
                System.err.println("panic 2");
            }
        }else if(notification_message.getType() == Message.Type.SHARED){

            // Activate all sharedlocks
            Set<String> set = this.shared_locks_set_map.get(notification_message.getLockpoint());

            // Ignore if this lockpoint has no members
            if (set == null) return;

            // Fire all cdls
            synchronized (this.shared_locks_set_map) {
                System.out.println("Shared locks: " + set);
                for (String lock_id : set) {
                    CountDownLatch cdl = this.lock_to_cdl_map.get(lock_id);
                    if (cdl != null) cdl.countDown();
                    else {
                        System.err.println("panic 9 - " + lock_id);
                    }
                }
            }
        }else if(notification_message.getType() == Message.Type.UNLOCK){

            System.out.println("UNLOCK ");

            // Make sure message is meant for this client
            if(client_id.equals(notification_message.getClientId().orElseThrow(NoSuchElementException::new))){

                // Check whose lock id this is for
                CountDownLatch cdl = this.lock_to_cdl_map.get(notification_message.getLockId().orElseThrow(
                        NoSuchElementException::new));

                // Panic if cdl is empty
                if (cdl == null) {
                    // Panic
                    System.err.println("panic 1: " +
                                       notification_message.getLockId() +
                                       " " +
                                       notification_message.getClientId() +
                                       " " +
                                       notification_message.getLockpoint());
                    return;
                }

                // Count down
                cdl.countDown();

            } else {
                // TODO refire?
                System.err.println("panic 8  " + notification_message.getClientId());
            }

        }else if(notification_message.getType() == Message.Type.LOCK){

            // Refire with new duration
            this.setRefireTimer(
                    notification_message.getLockpoint(),
                    notification_message.getLeaseTime().orElseThrow(() -> new RuntimeException("Empty lease time!"))
            );
        }
    }

    private void setRefireTimer(@Nonnull String lockpoint, long duration) {

        // Check if timer is set to indefinite
        if (duration == -1) duration = DEFAULT_LEASE_DURATION_SECONDS * 1000;

        // New Timertask
        TimerTask new_timer_task = new TimerTask() {
            @Override
            public void run() {
                lockpoint_to_timer.remove(lockpoint);
                refire(lockpoint);
            }
        };

        // Put it in the map
        TimerTask ret_task = this.lockpoint_to_timer.put(lockpoint, new_timer_task);

        // Cancel previous timer task
        if (ret_task != null) {
            ret_task.cancel();
        }

        // Start timer
        this.timer.schedule(new_timer_task, duration);
    }

    private void refire(@Nonnull String lockpoint) {

        long duration = this.performLongEval(
                "single_instance_refire",
                lockpoint,
                DEFAULT_LEASE_DURATION_SECONDS * 1000 + "",
                prefix
        );

        // If equals zero, then go ahead and launch another timertask to perform a very short wait
        if (duration == 0) setRefireTimer(lockpoint, (DEFAULT_LEASE_DURATION_SECONDS * 1000) / 4);

            // If equals to -1, means no expiration time on that lock, so refire to poll that lock
        else if (duration <= -1) setRefireTimer(lockpoint, DEFAULT_LEASE_DURATION_SECONDS * 1000);

            // Else, wait for that duration
        else setRefireTimer(lockpoint, duration);
    }

    /**
     * Comfy little class that holds time and time unit
     */
    private static class Duration {
        private long time;
        private TimeUnit unit;

        Duration(@Nonnegative long time, @Nonnull TimeUnit unit) {
            this.time = time;
            this.unit = unit;
        }

        @Nonnull
        synchronized TimeUnit getUnit() {
            return this.unit;
        }

        synchronized void set(@Nonnegative long time, @Nonnull TimeUnit unit) {
            this.time = time;
            this.unit = unit;
        }

        synchronized long getTime(@Nonnull TimeUnit unit) {
            return unit.convert(this.time, this.unit);
        }
    }
}