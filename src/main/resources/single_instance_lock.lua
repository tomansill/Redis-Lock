-- Input - lockpoint client_id lock_id is_fair first_attempt lock_lease lockwait_lease is_read_lock prefix

-- KEYS[1] lockpoint - name of lockpoint
-- KEYS[2] client_id - id of client
-- KEYS[3] lock_id - id of lock
-- KEYS[4] is_fair - true if the lock is fair, false if it is not
-- KEYS[5] first_attempt - true if first attempt at locking, false if this is not the first attempt
-- KEYS[6] lock_lease - lock lease in milliseconds
-- KEYS[7] lockwait_lease - lockwait lease in milliseconds
-- KEYS[8] is_read - true if lock is a readlock, false if it is a writelock
-- KEYS[9] prefix - prefix for lock namespaces
-- KEYS[10] trylock - true if the lock will not go in waitlist, false so it will enqueue in waitlist

-- Initialization
local lockpoint = KEYS[9] .. "lockpoint:" .. KEYS[1]
local client_id = KEYS[2]
local lock_id = KEYS[3]
local client_lock_id = client_id .. ":" .. lock_id
local is_fair = tonumber(KEYS[4])
local first_attempt = tonumber(KEYS[5])
local lock_lease_time = KEYS[6]
local lockwait_lease_time = KEYS[7]
local is_read_lock = tonumber(KEYS[8])
local lockcount = KEYS[9] .. "lockcount:" .. KEYS[1]
local lockwait = KEYS[9] .. "lockwait:" .. KEYS[1]
local lockpool = KEYS[9] .. "lockpool:" .. KEYS[1]
local trylock = tonumber(KEYS[10])

-- Check if fair and first time
if (first_attempt == 1) and (is_fair == 1) then

    -- Check if there's already locks waiting, if so, join them
    -- (Reason: so locks don't cut in the line thus enforcing fair locking policy)
    if (is_read_lock == 0) and (redis.call("LLEN", lockwait) ~= 0) then -- Writelock

        -- Add into the wait list
        redis.call("RPUSH", lockwait, client_lock_id)

        -- extend the expiration time
        redis.call("PEXPIRE", lockwait, lockwait_lease_time)

        return redis.call("PTTL", lockpoint) -- TODO catch -2 or -1

    elseif (is_read_lock == 1) and (redis.call("SCARD", lockpool) ~= 0) then -- Readlock

        -- Add into the wait pool
        redis.call("SADD", lockpool, client_lock_id)

        -- extend the expiration time
        redis.call("PEXPIRE", lockpool, lockwait_lease_time)

        return redis.call("PTTL", lockpoint) -- TODO catch -2 or -1

    elseif (trylock == 1) then
        return -1
    end

end

-- Lock it
local result = redis.call("GET", lockpoint)
if (not result) or (result == "dead") then -- Cleared to lock

    -- Switch on shared or read lock
    if (is_read_lock == 1) then -- Read lock
        redis.call("SET", lockpoint, "open", "PX", lock_lease_time)
        redis.call("SET", lockcount, "1")
        redis.call("DEL", lockpool); -- Remove the waiting pool so readlocks can go ahead and lock
        redis.call("PUBLISH", "lockchannel", "s:" .. client_lock_id) -- 's' event indicates shared lockpoint

    else -- Read lock
        redis.call("SET", lockpoint, "unique", "PX", lock_lease_time)
    end

    -- If this is not first attempt, then the lockwait needs to be popped
    if (first_attempt == 0) and (is_fair == 1) then
        redis.call("LPOP", lockwait)
        if (redis.call("LLEN", lockwait) == 0) then
            redis.call("DEL", lockwait)
        end
    end

    -- Publish lock lifetime
    redis.call("PUBLISH", "lockchannel", "l:" .. client_lock_id .. ":" .. lock_lease_time)

    return 0 -- 0 means success

else -- Lock failed

    -- If trylock, return immediately
    if trylock == 1 then return -1 end

    -- Switch on write or read locks
    if is_read_lock == 1 then -- Readlocks

        -- Check if lock is open for sharing
        -- If readlock is not fair, disregard "closed" state and lock anyways
        if (result == "open") or (is_fair == 0 and result == "closed") then -- the lockpoint is open for sharing

            -- Increment the ownership
            redis.call("INCR", lockpoint)

            -- Extend the lockpoint
            redis.call("PEXPIRE", lockpoint, lock_lease_time)

            -- Publish lock lifetime
            redis.call("PUBLISH", "lockchannel", "l:" .. client_lock_id .. ":" .. lock_lease_time)

            -- Success
            return 0 -- 0 means success

        else -- the lockpoint is not open for sharing - this means readlock is blocked

            -- Check if the waiting pool is open
            if redis.call("SCARD", lockpool) == 0 then -- pool is empty
                redis.call("RPUSH", lockwait, "S") -- "S" indicates that it's shared
            end

            -- Add in the pool
            redis.call("SADD", lockpool, client_lock_id)
            redis.call("PEXPIRE", lockpool, lockwait_lease_time)  -- extend the expiration time

            -- Get expiration time
            local expire = redis.call("PTTL", lockpoint);
            if expire <= 0 then expire = -1 end

            return expire
        end

    else -- Writelocks

        -- Enqueue in lockwait
        if (first_attempt == 1) and (is_fair == 1) then
            redis.call("RPUSH", lockwait, client_lock_id)
            redis.call("PEXPIRE", lockwait, lockwait_lease_time)  -- extend the expiration time
        end

        -- Get expiration time
        local expire = redis.call("PTTL", lockpoint);

        return expire
    end
end
