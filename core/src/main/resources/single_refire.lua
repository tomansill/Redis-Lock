-- Input - lockpoint

local key = "lockwait_refire:" .. KEYS[1]
local lockwait = "lockwait:" .. KEYS[1]

-- Only one get to access
if(redis.call("SETNX", key, ".")) then

    -- Drop the dead element on the lockwait
    redis.call("LPOP", lockwait)

    -- Get next element
    local element = redis.call("LINDEX", lockwait, 0)

    -- Release
    redis.call("DEL", key)

    -- Return it
    return element
end
return nil
