package com.tomansill.redis.jedis;

import com.tomansill.redis.exception.InvalidTypeException;
import com.tomansill.redis.exception.NoScriptFoundException;
import com.tomansill.redis.exception.ScriptHashErrorException;
import com.tomansill.redis.lock.AbstractRedisLockClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisDataException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Consumer;

import static com.tomansill.redis.lock.Utility.checkValueForNull;

/**
 *
 */
public class JedisLockClient extends AbstractRedisLockClient {

    /** Jedis connection */
    private Jedis connection = null;

    /** JedisPool */
    private JedisPool pool = null;

    /** Publish-Subscribe manager */
    private final JedisPubSubManager pubsub_manager;

	/** Jedis Lock Client constructor
	 *  @param jedis jedis connection
	 *  @param listener_jedis jedis connection for publish/subscribe listener. Do not reuse same connection for this
	 *  @throws IllegalArgumentException thrown if any one of the arguments are invalid
	 */
	public JedisLockClient(@Nonnull Jedis jedis, @Nonnull Jedis listener_jedis) throws IllegalArgumentException {
		this("", jedis, listener_jedis);
	}

	/** Jedis Lock Client constructor
	 *  @param prefix prefix to the lock names
	 *  @param jedis jedis connection
	 *  @param listener_jedis jedis connection for publish/subscribe listener. Do not reuse same connection for this
	 *  @throws IllegalArgumentException thrown if any one of the arguments are invalid
	 */
	public JedisLockClient(@Nonnull String prefix, @Nonnull Jedis jedis, @Nonnull Jedis listener_jedis) throws IllegalArgumentException {
		super(checkValueForNull(prefix, "prefix"));

        // Check parameter
	    if(jedis == listener_jedis) throw new IllegalArgumentException("jedis and listener_jedis are the same!");

	    // Assign
        this.connection = jedis;
        this.pubsub_manager = new JedisPubSubManager(listener_jedis);
    }

	/** Jedis Lock Client constructor
	 *  @param pool jedis connection pool
	 *  @throws IllegalArgumentException thrown if any one of the arguments are invalid
	 */
	public JedisLockClient(@Nonnull JedisPool pool) throws IllegalArgumentException {
		this("", pool);
	}

	/** Jedis Lock Client constructor
	 *  @param prefix prefix to the lock names
	 *  @param pool jedis connection pool
	 *  @throws IllegalArgumentException thrown if any one of the arguments are invalid
	 */
	public JedisLockClient(@Nonnull String prefix, @Nonnull JedisPool pool) throws IllegalArgumentException {
	    super(checkValueForNull(prefix, "prefix"));

        // Assign
        this.pool = pool;
        this.pubsub_manager = new JedisPubSubManager(pool.getResource());
    }

    /** Returns true if this client is connected to a cluster, false otherwise
     *  @return true if this client is connected to a cluster, false otherwise
     */
    @Override
    public boolean isCluster() {
        return false;
    }

	private static JedisDataException interpretException(@Nonnull JedisDataException exception, @Nullable String hash) throws ScriptHashErrorException, NoScriptFoundException {

		System.out.println(exception.getMessage());

		// Check if it's script error exception
		String pattern = "ERR Error running script (call to f_";
		int index = exception.getMessage().indexOf(pattern);
		if (index != -1) {

			// Get the end of offending script hash
			int end = exception.getMessage().indexOf("):", index + pattern.length());
			if (end == -1) throw exception;

			// Get the offending script hash
			String script_hash = exception.getMessage().substring(index + pattern.length(), end);

			// Get the line number
			String pattern2 = "@user_script:";
			int temp = exception.getMessage().indexOf(pattern2, end);
			if (temp == -1) throw exception;
			end = exception.getMessage().indexOf(":", temp + pattern2.length());
			String str_line_number = exception.getMessage().substring(temp + pattern2.length(), end);

			// Check if it is a number
			int line_number;
			try {
				line_number = Integer.parseInt(str_line_number);
			} catch (NumberFormatException ignored) {
				throw exception;
			}

			// Pick up the rest of message
			String message = exception.getMessage().substring(temp + pattern2.length() + str_line_number.length() + ": @user_script:".length() + str_line_number.length() + ": ".length());

			// Build and throw it exception
			throw new ScriptHashErrorException(message.trim(), script_hash.trim(), line_number);
		}

		// Check if it's script error exception
		pattern = "ERR Error compiling script (new function): user_script:";
		index = exception.getMessage().indexOf(pattern);
		if (index != -1) {

			// Get the line number
			int temp = exception.getMessage().indexOf(":", index + pattern.length());
			if (temp == -1) throw exception;
			String str_line_number = exception.getMessage().substring(index + pattern.length(), temp);

			// Check if it is a number
			int line_number;
			try {
				line_number = Integer.parseInt(str_line_number);
			} catch (NumberFormatException ignored) {
				throw exception;
			}

			// Pick up the rest of message
			String message = exception.getMessage().substring(temp + ": ".length());

			// Build and throw it exception
			throw new ScriptHashErrorException(message.trim(), "", line_number);
		}

		// Check if it's script not found exception
		pattern = "NOSCRIPT No matching script. Please use EVAL.";
		index = exception.getMessage().indexOf(pattern);
		if (index != -1) throw new NoScriptFoundException(hash);

		// throw it out as we dont know it
		return exception;
	}

    /** Loads script on the server and retrieve SHA1 digest of script
     *  @param script Lua script
     *  @return SHA1 digest of script
     */
	@Nonnull
	@Override
	protected String scriptLoad(@Nonnull String script) throws ScriptHashErrorException, NoScriptFoundException {

    	// Get Jedis connection
	    Jedis jedis;
	    if(this.connection != null) jedis = this.connection;
	    else jedis = this.pool.getResource();

	    // Fire it
	    try{
		    return jedis.scriptLoad(script);
	    }catch(JedisDataException e){
		    throw interpretException(e, null);
	    }finally{
	    	if(this.connection == null) jedis.close();
	    }
    }

    /** Evaluates and returns boolean value
     *  @param hash hash to Lua script
     *  @param args argument parameters
     *  @return boolean
     *  @throws InvalidTypeException thrown if script returns in wrong type
     */
    @Override
	protected boolean booleanEval(@Nonnull String hash, @Nonnull String... args) throws InvalidTypeException,
		    ScriptHashErrorException,
		    NoScriptFoundException{

	    // Get Jedis connection
	    Jedis jedis;
	    if(this.connection != null) jedis = this.connection;
	    else jedis = this.pool.getResource();

	    // Fire it
	    try{

	    	// Evaluate script
		    Object return_obj = jedis.evalsha(hash, args.length, args);

		    // Check if obj is null
		    if(return_obj == null) throw new InvalidTypeException("Boolean", "null");

		    // Check if object is an integer
		    if(return_obj instanceof Long){
			    long res = (Long) return_obj;
			    return res != 0;
		    }

		    // Check if object is a string (happens sometimes)
		    if(return_obj instanceof String){
		    	try{
				    long res = Long.parseLong((String) return_obj);
				    return res != 0;
			    }catch(NumberFormatException ignored){
				    throw new InvalidTypeException("Boolean", "String");
			    }
		    }

		    // Throw exception
		    throw new InvalidTypeException("Boolean", return_obj.getClass().getSimpleName());
	    }catch(JedisDataException e){
		    throw interpretException(e, hash);
	    }finally{
		    if(this.connection == null) jedis.close();
	    }
    }

    /** Evaluates and returns long value
     *  @param hash hash to Lua script
     *  @param args argument parameters
     *  @return long value
     *  @throws InvalidTypeException thrown if script returns in wrong type
     */
    @Override
	protected long longEval(@Nonnull String hash, @Nonnull String... args) throws InvalidTypeException, ScriptHashErrorException, NoScriptFoundException {

    	//System.out.println("hash: " + hash + " args: " + Arrays.toString(args));

	    // Get Jedis connection
	    Jedis jedis;
	    if(this.connection != null) jedis = this.connection;
	    else jedis = this.pool.getResource();

	    // Fire it
	    try{

		    // Evaluate script
		    Object return_obj = jedis.evalsha(hash, args.length, args);

		    // Check if obj is null
		    if(return_obj == null) throw new InvalidTypeException("Long", "null");

		    // Check if object is an integer
		    if(return_obj instanceof Long) return (Long) return_obj;

		    // Check if object is a string (happens sometimes)
		    if(return_obj instanceof String){
			    try{
				    return Long.parseLong((String) return_obj);
			    }catch(NumberFormatException ignored){
				    throw new InvalidTypeException("Long", "String");
			    }
		    }

		    // Throw exception
		    throw new InvalidTypeException("Long", return_obj.getClass().getSimpleName());
	    }catch(JedisDataException e){
		    throw interpretException(e, hash);
	    }finally{
		    if(this.connection == null) jedis.close();
	    }
    }

    /** Evaluates and returns boolean value
     *  @param hash hash to Lua script
     *  @param args argument parameters
     *  @return string
     *  @throws InvalidTypeException thrown if script returns in wrong type
     */
	@Nullable
	@Override
	protected String stringEval(@Nonnull String hash, @Nonnull String... args) throws InvalidTypeException, ScriptHashErrorException, NoScriptFoundException {

	    // Get Jedis connection
	    Jedis jedis;
	    if(this.connection != null) jedis = this.connection;
	    else jedis = this.pool.getResource();

	    // Fire it
	    try{

		    // Evaluate script
		    Object return_obj = jedis.evalsha(hash, args.length, args);

		    // Check if obj is null
		    if(return_obj == null) throw new InvalidTypeException("String", "null");

		    // Check if object is a string
		    if(return_obj instanceof String) return return_obj + "";

		    // Check if object is an integer
		    if(return_obj instanceof Long) return return_obj + "";

		    // Check if object is a double
		    if(return_obj instanceof Double) return return_obj + "";

		    // Throw exception
		    throw new InvalidTypeException("String", return_obj.getClass().getSimpleName());
	    }catch(JedisDataException e){
	    	throw interpretException(e, hash);
	    }finally{
		    if(this.connection == null) jedis.close();
	    }
    }

    /** Subscribes to channel
     *  @param channel  channel name
     *  @param function function to fire when new topic comes up
     */
    @Override
	public void subscribe(@Nonnull String channel, @Nonnull Consumer<String> function) {
		this.pubsub_manager.subscribe(channel, function);
     }

    /** Unsubscribe channel
     *  @param channel channel name
     */
    @Override
	public void unsubscribe(@Nonnull String channel) {
	    this.pubsub_manager.unsubscribe(channel);
    }
}
