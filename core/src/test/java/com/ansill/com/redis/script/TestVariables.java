package com.ansill.com.redis.script;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;

public interface TestVariables{

    /** Default lock lease */
    long LOCK_LEASE = 1_000L; // 1 second in milliseconds

    /** Default lockwait lease */
    long LOCKWAIT_LEASE = 30_000L; // 30 seconds in milliseconds

    /** TTL margin */
    long PTTL_MARGIN = 100L;

    /** Channel wait time duration */
    long CHANNEL_WAIT_TIME_DURATION = 500;

    /** Channel wait time unit */
    TimeUnit CHANNEL_WAIT_TIME_UNIT = TimeUnit.MILLISECONDS;

    @Nonnull
    static String getLockchannel(@Nonnull String prefix, @Nonnull String lockpoint){
        return prefix + ":lockchannel:" + lockpoint;
    }

    @Nonnull
    static String getLockpoint(@Nonnull String prefix, @Nonnull String lockpoint){
        return prefix + ":lockpoint:" + lockpoint;
    }

    @Nonnull
    static String getLockwait(@Nonnull String prefix, @Nonnull String lockpoint){
        return prefix + ":lockwait:" + lockpoint;
    }

    @Nonnull
    static String getLockpool(@Nonnull String prefix, @Nonnull String lockpoint){
        return prefix + ":lockpool:" + lockpoint;
    }

    @Nonnull
    static String getLockcount(@Nonnull String prefix, @Nonnull String lockpoint){
        return prefix + ":lockcount:" + lockpoint;
    }
}
