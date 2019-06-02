package com.ansill.com.redis.script;

import com.ansill.redis.lock.Utility;
import redis.clients.jedis.Jedis;

import javax.annotation.Nonnull;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class ScriptManager{

    /** Script hash */
    private ConcurrentHashMap<String,String> script_hash = new ConcurrentHashMap<>();

    /** Debug channel name */
    private String debug_channel = "debug_channel_" + Utility.generateRandomString(8);

    /** Prefix */
    private String prefix = "prefix-" + Utility.generateRandomString(4);

    @Nonnull
    String loadScript(@Nonnull Jedis connection, @Nonnull String script_path) throws IOException{

        // Catcher for exceptions
        AtomicReference<IOException> ioe = new AtomicReference<>(null);

        String hash = this.script_hash.computeIfAbsent(script_path, key -> {

            // Load script
            String script;
            try{
                script = Utility.loadStringFromResource(script_path).orElseThrow(() -> new FileNotFoundException(
                        script_path));
            }catch(IOException e){
                ioe.set(e);
                return null;
            }

            // Process script
            script = Utility.processScript(script, true, debug_channel);

            // Load script
            return connection.scriptLoad(script);
        });

        // Check if it has errored out
        if(hash == null && ioe.get() != null) throw ioe.get();
        if(hash == null) throw new RuntimeException("Hash is null!");

        // Return it
        return hash;
    }

    @Nonnull
    public String getDebugChannel(){
        return this.debug_channel;
    }

    @Nonnull
    public String getPrefix(){
        return this.prefix;
    }

}
