-- Input - lockpoint client_id lock_id is_fair first_attempt lock_lease lockwait_lease

-- Initialization
local lockpoint = "lockpoint:" .. KEYS[1]
local client_id = KEYS[2]
local lock_id = KEYS[3]
local client_lock_id = client_id .. ":" .. lock_id
local is_fair = tonumber(KEYS[4])
local first_attempt = tonumber(KEYS[5])
local lock_lease_time = KEYS[6]
local lockwait_lease_time = KEYS[7]
local lockwait = "lockwait:" .. KEYS[1]
local lockchannel = "lockchannel:" .. KEYS[1]

-- Check if fair and first time
if (first_attempt == 1) and (is_fair == 1) and (redis.call("LLEN", lockwait) ~= 0) then
    redis.call("RPUSH", lockwait, client_lock_id)
    redis.call("PEXPIRE", lockwait, lockwait_lease_time)
    return false
end

-- Lock it
if redis.call("SET", lockpoint, "ex", "NX", "PX", lock_lease_time) then
    -- Success
    -- If this is not first attempt, then the lockwait needs to be popped
    if (first_attempt == 0) and (is_fair == 1) then
        redis.call("LPOP", lockwait)
        if (redis.call("LLEN", lockwait) == 0) then
            redis.call("DEL", lockwait)
        end
    end
    return true
else
    -- Lock failed
    if (first_attempt == 1) and (is_fair == 1) then
        redis.call("RPUSH", lockwait, client_lock_id)
        redis.call("PEXPIRE", lockwait, lockwait_lease_time)
    end
    return false
end
