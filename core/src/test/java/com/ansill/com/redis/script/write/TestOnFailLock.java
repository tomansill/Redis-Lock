package com.ansill.com.redis.script.write;

import com.ansill.com.redis.script.SingleInstanceLock;
import com.ansill.com.redis.script.SingleInstanceTest;
import com.ansill.com.redis.script.TestVariables;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static com.ansill.redis.lock.Utility.generateRandomString;

class TestOnFailLock extends SingleInstanceTest{

    @Test
    void testUnfairWriteLock() throws InterruptedException, IOException{

        String lockpoint = generateRandomString(8);

        // Set up parameters
        SingleInstanceLock.Parameters successful_parameters = new SingleInstanceLock.Parameters(
                lockpoint,
                generateRandomString(8),
                generateRandomString(8),
                false,
                true,
                TestVariables.LOCK_LEASE,
                TestVariables.LOCKWAIT_LEASE,
                false,
                manager.getPrefix(),
                false
        );

        // Set up expected outputs - we expect a simple success
        SingleInstanceLock.ExpectedOutput output = SingleInstanceLock.ExpectedOutput.success(successful_parameters);

        // Fire it
        SingleInstanceLock.assertFunction(this.server, this.manager, successful_parameters, output);

        // Set up parameters
        SingleInstanceLock.Parameters failed_parameters = new SingleInstanceLock.Parameters(
                lockpoint,
                generateRandomString(8),
                generateRandomString(8),
                false,
                true,
                TestVariables.LOCK_LEASE,
                TestVariables.LOCKWAIT_LEASE,
                false,
                manager.getPrefix(),
                false
        );

        // Set up expected outputs - we expect a simple success
        output = SingleInstanceLock.ExpectedOutput.fail(successful_parameters, failed_parameters);

        // Fire it
        SingleInstanceLock.assertFunction(this.server, this.manager, failed_parameters, output);
    }

    @Test
    void testSimpleFairWriteLock() throws InterruptedException, IOException{

        // Set up parameters
        SingleInstanceLock.Parameters parameters = new SingleInstanceLock.Parameters(
                generateRandomString(8),
                generateRandomString(8),
                generateRandomString(8),
                true,
                true,
                TestVariables.LOCK_LEASE,
                TestVariables.LOCKWAIT_LEASE,
                false,
                manager.getPrefix(),
                false
        );

        // Set up expected outputs - we expect a simple success
        SingleInstanceLock.ExpectedOutput output = SingleInstanceLock.ExpectedOutput.success(parameters);

        // Fire it
        SingleInstanceLock.assertFunction(this.server, this.manager, parameters, output);
    }

    @Test
    void testSimpleUnfairWriteLockTry() throws InterruptedException, IOException{

        // Set up parameters
        SingleInstanceLock.Parameters parameters = new SingleInstanceLock.Parameters(
                generateRandomString(8),
                generateRandomString(8),
                generateRandomString(8),
                false,
                true,
                TestVariables.LOCK_LEASE,
                TestVariables.LOCKWAIT_LEASE,
                false,
                manager.getPrefix(),
                true
        );

        // Set up expected outputs - we expect a simple success
        SingleInstanceLock.ExpectedOutput output = SingleInstanceLock.ExpectedOutput.success(parameters);

        // Fire it
        SingleInstanceLock.assertFunction(this.server, this.manager, parameters, output);
    }

    @Test
    void testSimpleFairWriteLockTry() throws InterruptedException, IOException{

        // Set up parameters
        SingleInstanceLock.Parameters parameters = new SingleInstanceLock.Parameters(
                generateRandomString(8),
                generateRandomString(8),
                generateRandomString(8),
                true,
                true,
                TestVariables.LOCK_LEASE,
                TestVariables.LOCKWAIT_LEASE,
                false,
                manager.getPrefix(),
                true
        );

        // Set up expected outputs - we expect a simple success
        SingleInstanceLock.ExpectedOutput output = SingleInstanceLock.ExpectedOutput.success(parameters);

        // Fire it
        SingleInstanceLock.assertFunction(this.server, this.manager, parameters, output);
    }
}
