-- Input - lockpoint

-- Delete key and publish that lock has been released
redis.call("DEL", "lockpoint:" .. KEYS[1])

-- Get from lockwait
local element = redis.call("LINDEX", "lockwait:" .. KEYS[1], 0)

-- If empty, either nobody is waiting on queue or there's unfair locks waiting for it
if(not element) then
    element = "#"
else
    element = "o:" .. element
end

-- Call it
redis.call("PUBLISH", "lockchannel", element)

-- Return true
return 1