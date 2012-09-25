local fakeredis = require "fakeredis"
local R = fakeredis.new()

local printf = function(p,...)
  io.stdout:write(string.format(p,...)); io.stdout:flush()
end

local do_test = function(v1,v2)
  if v1 == v2 then
    printf(".")
  else
    error(string.format("expected %s, got %s",tostring(v2),tostring(v1)))
  end
end

-- strings

printf("strings ")
do_test(R:flushdb(),true)
do_test(R:get("foo"),nil)
do_test(R:set("foo","bar"),true)
do_test(R:get("foo"),"bar")
do_test(R:del("foo"),1)
do_test(R:get("foo"),nil)
print(" OK")

-- hashes

printf("hashes ")
do_test(R:get("foo"),nil)
do_test(R:hget("foo","bar"),nil)
do_test(R:hset("foo","spam","eggs"),true)
do_test(R:hget("foo","bar"),nil)
do_test(R:hset("foo","bar","baz"),true)
do_test(R:hget("foo","bar"),"baz")
do_test(R:hdel("foo","bar"),1)
do_test(R:hget("foo","bar"),nil)
do_test(R:del("foo"),0)
do_test(R:hget("foo","bar"),nil)
print(" OK")
