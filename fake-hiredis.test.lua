-- bits and pieces from the hiredis test suite

local hiredis = require "fake-hiredis"

--------------------------------------------------------------------------------

assert(type(hiredis.NIL == "table"))
assert(hiredis.NIL.name == "NIL")
assert(hiredis.NIL.type == hiredis.REPLY_NIL)
assert(tostring(hiredis.NIL) == "NIL")
assert(type(assert(getmetatable(hiredis.NIL))) == "string")

--------------------------------------------------------------------------------

assert(type(hiredis.status.OK == "table"))
assert(hiredis.status.OK.name == "OK")
assert(hiredis.status.OK.type == hiredis.REPLY_STATUS)
assert(tostring(hiredis.status.OK) == "OK")
assert(getmetatable(hiredis.status.OK) == getmetatable(hiredis.NIL))

-- deprecated backwards compatibility
-- assert(hiredis.OK == hiredis.status.OK)

--------------------------------------------------------------------------------

assert(type(hiredis.status.QUEUED == "table"))
assert(hiredis.status.QUEUED.name == "QUEUED")
assert(hiredis.status.QUEUED.type == hiredis.REPLY_STATUS)
assert(tostring(hiredis.status.QUEUED) == "QUEUED")
assert(getmetatable(hiredis.status.QUEUED) == getmetatable(hiredis.NIL))

-- deprecated backwards compatibility
-- assert(hiredis.QUEUED == hiredis.status.QUEUED)

--------------------------------------------------------------------------------

assert(type(hiredis.status.PONG == "table"))
assert(hiredis.status.PONG.name == "PONG")
assert(hiredis.status.PONG.type == hiredis.REPLY_STATUS)
assert(tostring(hiredis.status.PONG) == "PONG")
assert(getmetatable(hiredis.status.PONG) == getmetatable(hiredis.NIL))

-- deprecated backwards compatibility
-- assert(hiredis.PONG == hiredis.status.PONG)
