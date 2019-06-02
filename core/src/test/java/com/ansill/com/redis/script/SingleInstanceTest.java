package com.ansill.com.redis.script;

import com.ansill.redis.Subscription;
import com.ansill.redis.test.ServerUtility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class SingleInstanceTest implements TestEnvironment, TestVariables{

    protected ServerUtility.Server server;

    protected ScriptManager manager = new ScriptManager();

    protected Subscription debug_subscription;

    @BeforeEach
    void setUp(){
        this.server = SERVER.get();
        this.debug_subscription = this.server.getManager().subscribe(
                this.manager.getDebugChannel(),
                System.out::println
        );
    }

    @AfterEach
    void tearDown(){
        this.debug_subscription.cancel();
    }
}
