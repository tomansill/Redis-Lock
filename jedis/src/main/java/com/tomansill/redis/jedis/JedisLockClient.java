package com.tomansill.redis.jedis;

import com.tomansill.redis.exception.InvalidTypeException;
import com.tomansill.redis.lock.AbstractRedisLockClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.Arrays;
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
	public JedisLockClient(final Jedis jedis, final Jedis listener_jedis) throws IllegalArgumentException{
		this("", jedis, listener_jedis);
	}

	/** Jedis Lock Client constructor
	 *  @param prefix prefix to the lock names
	 *  @param jedis jedis connection
	 *  @param listener_jedis jedis connection for publish/subscribe listener. Do not reuse same connection for this
	 *  @throws IllegalArgumentException thrown if any one of the arguments are invalid
	 */
    public JedisLockClient(final String prefix, final Jedis jedis, final Jedis listener_jedis) throws IllegalArgumentException{
		super(checkValueForNull(prefix, "prefix"));

        // Check parameter
        if(jedis == null) throw new IllegalArgumentException("jedis is null");
	    if(listener_jedis == null) throw new IllegalArgumentException("listener_jedis is null");
	    if(jedis == listener_jedis) throw new IllegalArgumentException("jedis and listener_jedis are the same!");

	    // Assign
        this.connection = jedis;
        this.pubsub_manager = new JedisPubSubManager(listener_jedis);
    }

	/** Jedis Lock Client constructor
	 *  @param pool jedis connection pool
	 *  @throws IllegalArgumentException thrown if any one of the arguments are invalid
	 */
	public JedisLockClient(final JedisPool pool) throws IllegalArgumentException{
		this("", pool);
	}

	/** Jedis Lock Client constructor
	 *  @param prefix prefix to the lock names
	 *  @param pool jedis connection pool
	 *  @throws IllegalArgumentException thrown if any one of the arguments are invalid
	 */
    public JedisLockClient(final String prefix, final JedisPool pool) throws IllegalArgumentException{
	    super(checkValueForNull(prefix, "prefix"));

	    // Check parameter
        if(pool == null) throw new IllegalArgumentException("pool is null");

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

    /** Loads script on the server and retrieve SHA1 digest of script
     *  @param script Lua script
     *  @return SHA1 digest of script
     */
    @Override
    protected String scriptLoad(final String script){

    	// Get Jedis connection
	    Jedis jedis;
	    if(this.connection != null) jedis = this.connection;
	    else jedis = this.pool.getResource();

	    // Fire it
	    try{
		    return jedis.scriptLoad(script);
	    }catch(JedisDataException e){
	    	System.out.println(script);
	    	throw e;
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
    protected boolean booleanEval(final String hash, final String... args) throws InvalidTypeException{

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
    protected long longEval(final String hash, final String... args) throws InvalidTypeException{

    	System.out.println("hash: " + hash + " args: " + Arrays.toString(args));

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
    @Override
    protected String stringEval(final String hash, final String... args) throws InvalidTypeException {

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

	    }finally{
		    if(this.connection == null) jedis.close();
	    }
    }

    /** Subscribes to channel
     *  @param channel  channel name
     *  @param function function to fire when new topic comes up
     */
    @Override
    public void subscribe(final String channel, final Consumer<String> function){
		this.pubsub_manager.subscribe(channel, function);
    }

    /** Unsubscribe channel
     *  @param channel channel name
     */
    @Override
    public void unsubscribe(final String channel){
	    this.pubsub_manager.unsubscribe(channel);
    }
}
