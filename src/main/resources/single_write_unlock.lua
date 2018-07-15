-- Input - lockpoint

-- Delete key and publish that lock has been released
redis.call("DEL", "lockpoint:" .. KEYS[1])
local element = redis.call("LINDEX", "lockwait:" .. lockpoint, 0)
redis.call("PUBLISH", "lockchannel:" .. KEYS[1], element)
