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
local lockpool = KEYS[9] .. "lockwait:" .. KEYS[1]

-- Check if lockwait has been expired (or has been quietly deleted) (this mitigates the herd effect)
local expire = redis.call("PTTL", lockwait)
if expire == -2 then

    -- Extend the lockwait
    redis.call("PEXPIRE", lockwait_lease_time)

else
    return expire
end