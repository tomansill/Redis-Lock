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

    -- Get the next client_lock
    local element = redis.call("LINDEX", lockwait, 0)

    -- If empty, either nobody is waiting on queue or there's unfair locks waiting for it
    if(not element) then
        element = "#"
    else
        element = "o:" .. element
    end

    -- Call it
    redis.call("PUBLISH", "lockchannel", element)

else
    return expire
end