local hiredis = {
  REPLY_STRING = 1,
  REPLY_ARRAY = 2,
  REPLY_INTEGER = 3,
  REPLY_NIL = 4,
  REPLY_STATUS = 5,
  REPLY_ERROR = 6,
  ERR_IO = 1,
  ERR_OTHER = 2,
  ERR_EOF = 3,
  ERR_PROTOCOL = 4,
}

-- statuses
local reply_mt = {
  __tostring = function(t)
    return t.name
  end,
  __metatable = "lua-hiredis.const",
}

local mkreply = function(_type,name)
  local r = {
    name = name,
    ["type"] = hiredis["REPLY_" .. _type],
  }
  return setmetatable(r,reply_mt)
end

local status_t_mt = {
  __index = function(t,k)
    t[k] = mkreply("STATUS",k)
  end
}
hiredis.status = setmetatable({},status_t_mt)

hiredis.NIL = mkreply("NIL","NIL")

return hiredis
