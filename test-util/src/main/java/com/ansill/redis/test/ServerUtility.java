package com.ansill.redis.test;

import com.ansill.redis.JedisPubSubManager;
import redis.clients.jedis.Jedis;
import redis.embedded.RedisServer;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashSet;
import java.util.Set;

public final class ServerUtility {

    private static final Set<Integer> RESERVED_PORTS = new HashSet<>();

    private ServerUtility() {
    }

    @Nonnull
    public static Server getServer() {

        // Return new instance
        return new Server();
    }

    @SuppressWarnings("SameParameterValue")
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

    public final static class Server implements AutoCloseable{

        @Nonnull
        private final RedisServer server;

        private boolean running = true;

        @Nonnegative
        private final int port;

        @Nonnull
        private final com.ansill.redis.JedisPubSubManager manager;

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

            // Set up pub sub manager
            this.manager = new JedisPubSubManager(this.getHostname(), this.getPort());
        }

        @Nonnull
        public String getHostname(){
            return "localhost";
        }

        public int getPort(){
            return this.port;
        }

        @Nonnull
        public JedisPubSubManager getManager(){
            return this.manager;
        }

        @Nonnull
        public Jedis getConnection(){
            return new Jedis(this.getHostname(), this.getPort());
        }

        @Override
        public void close(){
            if(this.running){
                this.manager.close();
                this.server.stop();
                unreservePort(this.port);
            }
            this.running = false;
        }
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
