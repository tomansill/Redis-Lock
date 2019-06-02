package com.ansill.com.redis.script;

import com.ansill.redis.test.ServerUtility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.util.concurrent.atomic.AtomicReference;

public interface TestEnvironment{

    /** Server */
    AtomicReference<ServerUtility.Server> SERVER = new AtomicReference<>(null);

    @BeforeAll
    static void setUpEnvironment(){

        // Set up server connection
        SERVER.set(ServerUtility.getServer());
    }

    @AfterAll
    static void tearDownEnvironment(){
        SERVER.get().close();
    }
}
