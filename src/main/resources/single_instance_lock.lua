-- Input - lockpoint client_id lock_id is_fair first_attempt lock_lease lockwait_lease is_read_lock

-- Initialization
local lockpoint = "lockpoint:" .. KEYS[1]
local client_id = KEYS[2]
local lock_id = KEYS[3]
local client_lock_id = client_id .. ":" .. lock_id
local is_fair = tonumber(KEYS[4])
local first_attempt = tonumber(KEYS[5])
local lock_lease_time = KEYS[6]
local lockwait_lease_time = KEYS[7]
local is_read_lock = tonumber(KEYS[8])
local lockcount = "lockcount:" .. KEYS[1]
local lockwait = "lockwait:" .. KEYS[1]
local lockpool = "lockwait:" .. KEYS[1]

-- Check if fair and first time
if (first_attempt == 1) and (is_fair == 1) then

    if (is_read_lock == 0) and (redis.call("LLEN", lockwait) ~= 0) then -- Writelock
        redis.call("RPUSH", lockwait, client_lock_id)
        redis.call("PEXPIRE", lockwait, lockwait_lease_time) -- extend the expiration time

        -- Get expiration time
        local expire = redis.call("PTTL", lockpoint);
        if expire <= 0 then expire = -1 end

        return expire

    elseif (redis.call("SCARD", lockpool) ~= 0) then -- Readlock
        redis.call("SADD", lockpool, client_lock_id)
        redis.call("PEXPIRE", lockpool, lockwait_lease_time)  -- extend the expiration time

        -- Get expiration time
        local expire = redis.call("PTTL", lockpoint);
        if expire <= 0 then expire = -1 end

        return expire
    end

end

-- Notify others that the lock has been picked up
if first_attempt == 0 then
    redis.call("PUBLISH", "lockchannel", "c:" .. client_lock_id)
end

-- Lock it
if redis.call("SET", lockpoint, "unique", "NX", "PX", lock_lease_time) then -- Lock success

    -- If this is not first attempt, then the lockwait needs to be popped
    if (first_attempt == 0) and (is_fair == 1) then
        redis.call("LPOP", lockwait)
        if (redis.call("LLEN", lockwait) == 0) then
            redis.call("DEL", lockwait)
        end
    end

    -- If shared, then lock needs to be downgraded from writelock to readlock and open the readlock
    if (is_read_lock == 1) then
        redis.call("SET", lockpoint, "open") -- TODO can this go to the original SET call?
        redis.call("SET", lockcount, "1")
        redis.call("PUBLISH", "lockchannel", "s:" .. client_lock_id) -- 's' event indicates shared lock
    end

    return 1

else -- Lock failed

    -- Check if point is shared
    if redis.call("GET", lockpoint) ~= "unique" then -- the lockpoint is open for sharing

        -- Increment the ownership
        redis.call("INCR", lockpoint)

        -- Success
        return 1

    else -- the lockpoint is not open for sharing

        -- Check if the waiting pool is open
        if redis.call("SCARD", lockpool) == 0 then -- pool is empty
            redis.call("RPUSH", lockwait, "S") -- "S" indicates that it's shared
        end

        -- Add in the pool
        redis.call("SADD", lockpool, client_lock_id)
        redis.call("PEXPIRE", lockpool, lockwait_lease_time)  -- extend the expiration time

        -- Failure
        -- Get expiration time
        local expire = redis.call("PTTL", lockpoint);
        if expire <= 0 then expire = -1 end

        return expire
    end

    -- Enqueue in lockwait (writelocks only)
    if (first_attempt == 1) and (is_fair == 1) then
        redis.call("RPUSH", lockwait, client_lock_id)
        redis.call("PEXPIRE", lockwait, lockwait_lease_time)  -- extend the expiration time
    end

    -- Failure
    -- Get expiration time
    local expire = redis.call("PTTL", lockpoint);
    if expire <= 0 then expire = -1 end

    return expire
end
