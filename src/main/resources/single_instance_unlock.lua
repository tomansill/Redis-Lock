-- Input - lockpoint is_read_lock prefix is_owner

-- lockpoint - name of lockpoint
-- is_read_lock - true if lock is a readlock, false if it is a writelock
-- prefix - prefix for lock namespaces
-- is_owner - true if this lock owns the sharedlock

-- Initialization
local lockpoint = KEYS[3] .. "lockpoint:" .. KEYS[1]
local is_read_lock = tonumber(KEYS[2])
local lockcount = KEYS[3] .. "lockcount:" .. KEYS[1]
local lockwait = KEYS[3] .. "lockwait:" .. KEYS[1]
local is_owner = tonumber(KEYS[4])
local lockchannel = KEYS[3] .. "channel:" .. KEYS[1]

-- Check if readlock
if is_read_lock == 1 then

    -- If lock owner, close the lockpoint
    if is_owner == 1 then
        local ttl = redis.call("PTTL", lockpoint);
        redis.call("SET", lockpoint, "closed");
        redis.call("PEXPIRE", lockpoint, ttl)
    end

    -- Decrement the lockcount
    local ownership_count = redis.call("DECR", lockcount);

    -- If the ownership count is zero, then lockpoint and lockcount should be deleted, resulting in unlocking it
    if(ownership_count ~= 0) then
        return 0; -- exit the function early
    else
        redis.call("DEL", lockcount);
    end
end

-- Delete key and publish that lock has been released
redis.call("DEL", lockpoint)

-- Get from lockwait (no pop)
local element = redis.call("LINDEX", lockwait, 0)

-- If empty, either nobody is waiting on queue or there's unfair locks waiting for it
if(not element) then
    element = "#" .. ":" .. KEYS[1]
else
    if(element == 's') then
        element = "s:" .. KEYS[1]
    else
        element = "u:" .. element .. ":" .. KEYS[1]
    end
end

-- Call it
redis.call("PUBLISH", lockchannel, element)

-- Return true
return 1
