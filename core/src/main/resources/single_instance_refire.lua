-- Input - lockpoint lockwait_lease_time prefix

-- KEYS[1] lockpoint - name of lockpoint
-- KEYS[2] lockwait_lease - lockwait lease in milliseconds
-- KEYS[3] prefix - prefix for lock namespaces

-- Initialization
local lockpoint = KEYS[3] .. "lockpoint:" .. KEYS[1]
local lockwait_lease_time = KEYS[1]
local lockwait = KEYS[3] .. "lockwait:" .. KEYS[1]
local lockpool = KEYS[3] .. "lockpool:" .. KEYS[1]
local lockchannel = KEYS[3] .. "channel:" .. KEYS[1]

-- Check if lockwait has been expired (or has been quietly deleted)
-- this mitigates the herd effect)
local expire = redis.call("PTTL", lockpoint)
if expire == -2 then

    -- Pop the lockwait
    local popped = redis.call("LPOP", lockwait)

    -- Check if shared lockpoint. If it is shared, remove the lockpool
    if popped == "S" then redis.call("DEL", lockpool) end

    -- Extend the lockwait
    redis.call("PEXPIRE", lockwait_lease_time)

    -- Declare this lockpoint dead (to prevent herd effect of this function) so it can get picked up by next lock
    redis.call("SET", lockpoint, "dead")
    redis.call("PEXPIRE", lockpoint, lockwait_lease_time)

    -- Get the next client lock
    local element = redis.call("LINDEX", lockwait, 0)

    -- If empty, either nobody is waiting on queue or there's unfair locks waiting for it
    if(not element) then
        element = "#" .. ":" .. KEYS[1]
    else
        element = "o:" .. element .. ":" .. KEYS[1]
    end

    -- Call it
    redis.call("PUBLISH", lockchannel, element)

    return 0 -- success

else
    return expire
end