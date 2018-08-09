package com.tomansill.redis.jedis;

import com.tomansill.redis.lock.AbstractRedisLockClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

public class JedisLockClient extends AbstractRedisLockClient {

    /** */
    private Jedis connection = null;

    /** */
    private JedisPool pool = null;

    /** */
    private Jedis listener_connection = null;

    /** */
    private final ConcurrentHashMap<String,JedisPubSub> listeners = new ConcurrentHashMap<>();

    /**
     * @param jedis
     */
    public JedisLockClient(final Jedis jedis){

        // Check parameter
        if(jedis == null) throw new IllegalArgumentException("jedis is null");

        // Assign
        this.connection = jedis;
    }

    /**
     * @param pool
     */
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

        //System.out.println("script " + script);

        // TODO
        String result;
        if(this.connection != null){
            result = this.connection.scriptLoad(script);
        }else{
            try(Jedis jedis = this.pool.getResource()){
                result = jedis.scriptLoad(script);
            }
        }

        System.out.println("sha " + result + " script:\n" + script);

        return result;
    }

    /**
     * Evaluates and returns boolean value
     *
     * @param hash hash to Lua script
     * @param args argument parameters
     * @return boolean
     */
    @Override
    protected boolean booleanEval(final String hash, final String... args) {
        if(this.connection != null){
            Object return_obj = this.connection.evalsha(hash, args.length, args);
            if(return_obj == null) throw new RuntimeException("returned null with script: " + hash); //TODO
            else if(return_obj instanceof Long){
                long res = (Long) return_obj;
                return res != 0;
            }else throw new RuntimeException(return_obj.getClass().getName()); //TODO
        }else{
            try(Jedis jedis = this.pool.getResource()){
                Object return_obj = jedis.evalsha(hash, args.length, args);
                if(return_obj == null) throw new RuntimeException("returned null with script: " + hash); //TODO
                else if(return_obj instanceof Long){
                    long res = (Long) return_obj;
                    return res != 0;
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
     */
    @Override
    protected String stringEval(final String hash, final String... args) {
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
    protected synchronized String subscribe(final String channel, final Predicate<String> function) {

        //System.out.println("subscribe(channel=" + channel + ")");

        // Get function hash
        String id = function.hashCode() + ""; //TODO what for?

        // Get or create new Pubsub
        if(!this.listeners.containsKey(channel)){
            this.listeners.put(channel, new Listener(function));
        }
        final JedisPubSub listener = this.listeners.get(channel);

        // Hook it up
        if(this.connection != null){
            final Jedis con = this.connection;
            new Thread(() -> {
                //System.out.println("run");
                con.subscribe(listener, channel);
                //System.out.println("exit");
            }).start();
        }else{
            this.listener_connection = this.pool.getResource();
            final Jedis con = this.listener_connection;
            new Thread(() -> {
                //System.out.println("run");
                con.subscribe(listener, channel);
                //System.out.println("exit");
            }).start();
        }

        return id;
    }

    /**
     * Unsubscribe channel
     *
     * @param channel       channel name
     * @param function_hash hash to identify function on the channel
     */
    @Override
    protected void unsubscribe(final String channel, final String function_hash) {

        //System.out.println("unsubscribe(channel=" + channel + ")");

        this.listeners.remove(channel).unsubscribe();

        //System.out.println("toredown");

    }

    private class Listener extends JedisPubSub{

        private Predicate<String> function;

        AtomicLong counter = new AtomicLong();

        public Listener(Predicate<String> function){
            this.function = function;
            //System.out.println("Listener " + counter.incrementAndGet());
        }

        public void onMessage(final String channel, final String message){
            //System.out.println("MESSAGE: " + message);
            this.function.test(message);
        }
    }
}
