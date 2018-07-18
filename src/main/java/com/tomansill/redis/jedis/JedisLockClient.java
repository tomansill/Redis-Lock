package com.tomansill.redis.jedis;

import com.tomansill.redis.exception.NoScriptFoundException;
import com.tomansill.redis.lock.AbstractRedisLockClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

public class JedisLockClient extends AbstractRedisLockClient {

    /** */
    private Jedis connection = null;

    /** */
    private JedisPool pool = null;

    /** */
    private Jedis listener = null;

    /** */
    private final ConcurrentHashMap<String,JedisPubSub> listeners = new ConcurrentHashMap<>();

    public JedisLockClient(final Jedis jedis){

        // Check parameter
        if(jedis == null) throw new IllegalArgumentException("jedis is null");

        // Assign
        this.connection = jedis;
    }

    public JedisLockClient(final JedisPool pool){

        // Check parameter
        if(pool == null) throw new IllegalArgumentException("pool is null");

        // Assign
        this.pool = pool;
    }

    /** Returns true if this client is connected to a cluster, false otherwise
     *  @return true if this client is connected to a cluster, false otherwise
     */
    @Override
    public boolean isCluster() {
        return false;
    }

    /**
     * Loads script on the server and retrieve SHA1 digest of script
     * @param script Lua script
     * @return SHA1 digest of script
     */
    @Override
    protected String scriptLoad(final String script) {

        System.out.println("script " + script);

        // TODO
        String result = null;
        if(this.connection != null){
            result = this.connection.scriptLoad(script);
        }else{
            try(Jedis jedis = this.pool.getResource()){
                result = jedis.scriptLoad(script);
            }
        }

        System.out.println("sha " + result);


        return result;
    }

    /**
     * Evaluates and returns boolean value
     *
     * @param hash hash to Lua script
     * @param args argument parameters
     * @return boolean
     * @throws NoScriptFoundException thrown if the script to the corresponding hash cannot be found on the database
     */
    @Override
    protected boolean booleanEval(final String hash, final String... args) throws NoScriptFoundException {
        for (String str : args){
            System.out.println(str);
        }

        if(this.connection != null){
            Object return_obj = this.connection.evalsha(hash, args.length, args);
            if(return_obj instanceof Boolean){
                return ((Boolean)return_obj).booleanValue();
            }else throw new RuntimeException(return_obj.getClass().getName()); //TODO
        }else{
            try(Jedis jedis = this.pool.getResource()){
                Object return_obj = jedis.evalsha(hash, args.length, args);
                if(return_obj == null) throw new RuntimeException("returned null"); //TODO
                else if(return_obj instanceof Long){
                    long res = ((Long)return_obj).longValue();
                    if(res == 0) return false;
                    else return true;
                }else throw new RuntimeException(return_obj.getClass().getName()); //TODO
            }
        }
    }

    /**
     * Evaluates and returns boolean value
     *
     * @param hash hash to Lua script
     * @param args argument parameters
     * @return string
     * @throws NoScriptFoundException thrown if the script to the corresponding hash cannot be found on the database
     */
    @Override
    protected String stringEval(final String hash, final String... args) throws NoScriptFoundException {
        if(this.connection != null){
            Object return_obj = this.connection.evalsha(hash, args.length, args);
            if(return_obj instanceof String){
                return (String)return_obj;
            }else throw new RuntimeException(return_obj.getClass().getName()); //TODO
        }else{
            try(Jedis jedis = this.pool.getResource()){
                Object return_obj = jedis.evalsha(hash, args.length, args);
                if(return_obj instanceof String){
                    return (String)return_obj;
                }else throw new RuntimeException(return_obj.getClass().getName()); //TODO
            }
        }
    }

    /**
     * Subscribes to channel
     *
     * @param channel  channel name
     * @param function function to fire when new topic comes up
     * @return string hash of function
     */
    @Override
    protected String subscribe(final String channel, final Predicate<String> function) {

        // Get function hash
        String id = function.hashCode() + "";

        // Create listener
        Listener l = new Listener(function);

        // Add into list
        this.listeners.put(channel, l);

        // Hook it up
        if(this.connection != null){
            final Jedis con = this.connection;
            new Thread(() -> {
                System.out.println("run");
                con.subscribe(l, channel);
                System.out.println("exit");
            }).start();
        }else{
            this.listener = this.pool.getResource();
            final Jedis con = this.listener;
            new Thread(() -> {
                System.out.println("run");
                con.subscribe(l, channel);
                System.out.println("exit");
            }).start();
        }

        return id;
    }

    /**
     * Unsunscribes channel
     *
     * @param channel       channel name
     * @param function_hash hash to identify function on the channel
     */
    @Override
    protected void unsubscribe(final String channel, final String function_hash) {

        System.out.println("unsubscribe");

        this.listeners.remove(function_hash).unsubscribe();

        System.out.println("toredown");

    }

    private class Listener extends JedisPubSub{

        private Predicate<String> function;

        public Listener(Predicate<String> function){
            this.function = function;
            System.out.println("Listener");
        }

        public void onMessage(final String channel, final String message){
            System.out.println("MESSAGE: " + message);
            this.function.test(message);
        }
    }
}
