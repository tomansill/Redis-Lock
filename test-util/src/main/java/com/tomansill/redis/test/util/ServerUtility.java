package com.tomansill.redis.test.util;

import redis.clients.jedis.Jedis;
import redis.embedded.RedisServer;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.*;
import java.util.concurrent.*;

public final class ServerUtility {

    private static final Set<Integer> RESERVED_PORTS = new HashSet<>();

    private ServerUtility() {
    }

    @Nonnull
    public static Server getServer() {

        // Return new instance
        return new Server();
    }

    public final static class Server implements AutoCloseable{

        @Nonnull
        private final RedisServer server;

        private boolean running = true;

        @Nonnegative
        private final int port;

        @Nonnull
        private final JedisPubSubManager manager;

        @Nonnull
        private final Jedis pubsub_conn;

        @Nonnull
        private final ConcurrentHashMap<String,BlockingQueue<String>> streams = new ConcurrentHashMap<>();

        @Nonnull
        private final Map<String,ResetableCountDownLatch> rcdl_streams = new HashMap<>();

        private Server() {

            // Find a port
            this.port = getNextOpenPort(6379);

            // Start it up
            try {
                this.server = new RedisServer(port);
                this.server.start();
            }catch (IOException e){
                // Wrap it
                throw new RuntimeException(e);
            }

            // Set up pubsub connection
            this.pubsub_conn = this.getConnection();

            // Set up pub sub manager
            this.manager = new JedisPubSubManager(this.pubsub_conn);
        }

        @Nonnull
        public Jedis getConnection(){
            return new Jedis("localhost", this.port);
        }

        @Nonnull
        public BlockingQueue<String> getStream(@Nonnull String channel){
            return this.streams.computeIfAbsent(channel, (channel_name -> {
                BlockingQueue<String> queue = new LinkedBlockingQueue<>();
                this.manager.subscribe(channel, queue::add);
                //this.rcdl_streams.put(channel, new ResetableCountDownLatch(0))
                return queue;
            }));
        }

        @Override
        public void close(){
            if(this.running){
                this.manager.unsubscribeAll();
                this.pubsub_conn.close();
                this.server.stop();
                unreservePort(this.port);
            }
            this.running = false;
        }
    }

    @Nonnegative
    private static synchronized int getNextOpenPort(@Nonnegative int start){

        // Loop until maximum possible
        for(int port = start; port < Short.MAX_VALUE; port++){

            // Make sure not already reserved
            if(RESERVED_PORTS.contains(port)) continue;

            // Check if it's open
            if(!isPortOpen(port)) continue;

            // If it is indeed open, reserve it
            RESERVED_PORTS.add(port);

            // Return it
            return port;
        }

        // Throw it
        throw new RuntimeException("Cannot find any open ports!");
    }

    private static synchronized void unreservePort(@Nonnegative int port){
        RESERVED_PORTS.remove(port);
    }

    private static boolean isPortOpen(@Nonnegative int port){
        try(ServerSocket ignored = new ServerSocket(port)){
            return true;
        } catch (IOException e) {
            return false;
        }
    }

}
