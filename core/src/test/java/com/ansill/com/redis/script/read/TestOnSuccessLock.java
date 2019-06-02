package com.ansill.com.redis.script.read;

import com.ansill.com.redis.script.SingleInstanceLock;
import com.ansill.com.redis.script.SingleInstanceTest;
import com.ansill.com.redis.script.TestVariables;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static com.ansill.redis.lock.Utility.generateRandomString;

class TestOnSuccessLock extends SingleInstanceTest{

    @Test
    void testSimpleUnfairReadLock() throws InterruptedException, IOException{

        // Set up parameters
        SingleInstanceLock.Parameters parameters = new SingleInstanceLock.Parameters(
                generateRandomString(8),
                generateRandomString(8),
                generateRandomString(8),
                false,
                true,
                TestVariables.LOCK_LEASE,
                TestVariables.LOCKWAIT_LEASE,
                true,
                manager.getPrefix(),
                false
        );

        // Set up expected outputs - we expect a simple success
        SingleInstanceLock.ExpectedOutput output = SingleInstanceLock.ExpectedOutput.success(parameters);

        // Fire it
        SingleInstanceLock.assertFunction(this.server, this.manager, parameters, output);
    }

    @Test
    void testSimpleFairReadLock() throws InterruptedException, IOException{

        // Set up parameters
        SingleInstanceLock.Parameters parameters = new SingleInstanceLock.Parameters(
                generateRandomString(8),
                generateRandomString(8),
                generateRandomString(8),
                true,
                true,
                TestVariables.LOCK_LEASE,
                TestVariables.LOCKWAIT_LEASE,
                true,
                manager.getPrefix(),
                false
        );

        // Set up expected outputs - we expect a simple success
        SingleInstanceLock.ExpectedOutput output = SingleInstanceLock.ExpectedOutput.success(parameters);

        // Fire it
        SingleInstanceLock.assertFunction(this.server, this.manager, parameters, output);
    }

    @Test
    void testSimpleUnfairReadLockTry() throws InterruptedException, IOException{

        // Set up parameters
        SingleInstanceLock.Parameters parameters = new SingleInstanceLock.Parameters(
                generateRandomString(8),
                generateRandomString(8),
                generateRandomString(8),
                false,
                true,
                TestVariables.LOCK_LEASE,
                TestVariables.LOCKWAIT_LEASE,
                true,
                manager.getPrefix(),
                true
        );

        // Set up expected outputs - we expect a simple success
        SingleInstanceLock.ExpectedOutput output = SingleInstanceLock.ExpectedOutput.success(parameters);

        // Fire it
        SingleInstanceLock.assertFunction(this.server, this.manager, parameters, output);
    }

    @Test
    void testSimpleFairReadLockTry() throws InterruptedException, IOException{

        // Set up parameters
        SingleInstanceLock.Parameters parameters = new SingleInstanceLock.Parameters(
                generateRandomString(8),
                generateRandomString(8),
                generateRandomString(8),
                true,
                true,
                TestVariables.LOCK_LEASE,
                TestVariables.LOCKWAIT_LEASE,
                true,
                manager.getPrefix(),
                true
        );

        // Set up expected outputs - we expect a simple success
        SingleInstanceLock.ExpectedOutput output = SingleInstanceLock.ExpectedOutput.success(parameters);

        // Fire it
        SingleInstanceLock.assertFunction(this.server, this.manager, parameters, output);
    }

    @Test
    void testFairReadLockOnExistingReadLock() throws InterruptedException, IOException{

        String lockpoint = generateRandomString(8);

        // Set up parameters
        SingleInstanceLock.Parameters parameters = new SingleInstanceLock.Parameters(
                lockpoint,
                generateRandomString(8),
                generateRandomString(8),
                true,
                true,
                TestVariables.LOCK_LEASE,
                TestVariables.LOCKWAIT_LEASE,
                true,
                manager.getPrefix(),
                false
        );

        // Set up expected outputs - we expect a simple success
        SingleInstanceLock.ExpectedOutput output = SingleInstanceLock.ExpectedOutput.success(parameters);

        // Fire it
        SingleInstanceLock.assertFunction(this.server, this.manager, parameters, output);

        // Do another read lock

        // Set up parameters
        parameters = new SingleInstanceLock.Parameters(
                lockpoint,
                generateRandomString(8),
                generateRandomString(8),
                true,
                true,
                TestVariables.LOCK_LEASE,
                TestVariables.LOCKWAIT_LEASE,
                true,
                manager.getPrefix(),
                false
        );

        // Set up expected outputs - we expect a simple success
        output = SingleInstanceLock.ExpectedOutput.readSuccess(2, parameters);

        // Fire it
        SingleInstanceLock.assertFunction(this.server, this.manager, parameters, output);

        // Set up parameters
        parameters = new SingleInstanceLock.Parameters(
                lockpoint,
                generateRandomString(8),
                generateRandomString(8),
                true,
                true,
                TestVariables.LOCK_LEASE,
                TestVariables.LOCKWAIT_LEASE,
                true,
                manager.getPrefix(),
                false
        );

        // Set up expected outputs - we expect a simple success
        output = SingleInstanceLock.ExpectedOutput.readSuccess(3, parameters);

        // Fire it
        SingleInstanceLock.assertFunction(this.server, this.manager, parameters, output);
    }

    @Test
    void testFairReadLockOnExistingReadLockTry() throws InterruptedException, IOException{

        String lockpoint = generateRandomString(8);

        // Set up parameters
        SingleInstanceLock.Parameters parameters = new SingleInstanceLock.Parameters(
                lockpoint,
                generateRandomString(8),
                generateRandomString(8),
                true,
                true,
                TestVariables.LOCK_LEASE,
                TestVariables.LOCKWAIT_LEASE,
                true,
                manager.getPrefix(),
                true
        );

        // Set up expected outputs - we expect a simple success
        SingleInstanceLock.ExpectedOutput output = SingleInstanceLock.ExpectedOutput.success(parameters);

        // Fire it
        SingleInstanceLock.assertFunction(this.server, this.manager, parameters, output);

        // Do another read lock

        // Set up parameters
        parameters = new SingleInstanceLock.Parameters(
                lockpoint,
                generateRandomString(8),
                generateRandomString(8),
                true,
                true,
                TestVariables.LOCK_LEASE,
                TestVariables.LOCKWAIT_LEASE,
                true,
                manager.getPrefix(),
                true
        );

        // Set up expected outputs - we expect a simple success
        output = SingleInstanceLock.ExpectedOutput.readSuccess(2, parameters);

        // Fire it
        SingleInstanceLock.assertFunction(this.server, this.manager, parameters, output);

        // Set up parameters
        parameters = new SingleInstanceLock.Parameters(
                lockpoint,
                generateRandomString(8),
                generateRandomString(8),
                true,
                true,
                TestVariables.LOCK_LEASE,
                TestVariables.LOCKWAIT_LEASE,
                true,
                manager.getPrefix(),
                true
        );

        // Set up expected outputs - we expect a simple success
        output = SingleInstanceLock.ExpectedOutput.readSuccess(3, parameters);

        // Fire it
        SingleInstanceLock.assertFunction(this.server, this.manager, parameters, output);
    }
}
