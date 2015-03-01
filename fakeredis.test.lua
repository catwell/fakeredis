-- WARNING: setting this to true will erase your local Redis DB.
local TEST_REDIS_LUA = false

local unpack = table.unpack or unpack

local cwtest = require "cwtest"
local fakeredis = require "fakeredis"

local T = cwtest.new()

local R
if TEST_REDIS_LUA then
  local redis = require "redis"
  R = redis.connect("127.0.0.1", 6379)
else
  local fakeredis = require "fakeredis"
  R = fakeredis.new()
end

T.rk_nil = function(self,k)
  local r
  if (
    (R:get(k) == nil) and
    (R:type(k) == "none") and
    (R:exists(k) == false)
  ) then
    r = self.pass_tpl(self," (value for %s is nil)",k)
  else
    r = self.fail_tpl(self," (value for %s is not nil)",k)
  end
  return r
end

T.rk_nil_hash = function(self,k,k2)
  local r
  if (
    (R:hget(k,k2) == nil) and
    (R:hexists(k,k2) == false)
  ) then
    r = self.pass_tpl(self," (value for %s[%s] is nil)",k,k2)
  else
    r = self.fail_tpl(self," (value for %s[%s] is not nil)",k,k2)
  end
  return r
end

--- strings (and some key commands)

T:start("strings"); do
  T:eq( R:flushdb(), true )
  T:eq( R:randomkey(), nil )
  T:rk_nil( "foo" )
  T:eq( R:strlen("foo"), 0 )
  T:eq( R:set("foo","bar"), true )
  T:eq( R:exists("foo"), true )
  T:eq( R:get("foo"), "bar" )
  T:eq( R:type("foo"), "string" )
  T:eq( R:strlen("foo"), 3 )
  T:eq( R:randomkey(), "foo" )
  T:eq( R:del("foo"), 1 )
  T:rk_nil( "foo" )
  T:eq( R:hset("foo","spam","eggs"), true )
  T:eq( R:type("foo"), "hash" )
  T:eq( R:set("foo","bar"), true )
  T:eq( R:get("foo"), "bar" )
  T:eq( R:type("foo"), "string" )
  T:eq( R:renamenx("foo","chunky"), true )
  T:eq( R:set("spam","eggs"), true )
  T:eq( R:renamenx("chunky","spam"), false )
  T:eq( R:get("spam"), "eggs" )
  T:eq( R:rename("chunky","spam"), true )
  T:rk_nil( "chunky" )
  T:eq( R:get("spam"), "bar" )
  T:eq( R:renamenx("spam","foo"), true )
  T:rk_nil( "spam" )
  T:eq( R:get("foo"), "bar" )
  T:eq( R:append("foo",""), 3 )
  T:eq( R:append("foo","bar"), 6 )
  T:eq( R:get("foo"), "barbar" )
  T:eq( R:append("spam","eggs"), 4 )
  T:eq( R:get("spam"), "eggs" )
  T:eq( R:mget("foo","chunky","spam"), {"barbar",nil,"eggs"} )
  T:eq( R:mset("chunky","bacon","foo","bar"), true )
  T:eq( R:mget("foo","chunky","spam"), {"bar","bacon","eggs"} )
  T:eq( R:setnx("chunky","bacon"), false )
  T:eq( R:del("chunky"), 1 )
  T:eq( R:setnx("chunky","bacon"), true )
  T:eq( R:get("chunky"), "bacon" )
  T:eq( R:del("chunky"), 1 )
  T:eq( R:del("spam"), 1 )
  T:eq( R:msetnx("chunky","bacon","foo","bar"), false )
  T:eq( R:mget("foo","chunky","spam"), {"bar",nil,nil} )
  T:eq( R:del("foo"), 1 )
  T:eq( R:msetnx("chunky","bacon","foo","bar"), true )
  T:eq( R:mget("foo","chunky","spam"), {"bar","bacon",nil} )
  T:eq( R:getset("foo","foobar"), "bar" )
  T:eq( R:getset("spam","eggs"), nil )
  T:eq( R:set("foo","This is a string"), true )
  T:eq( R:getrange("foo",100,150), "" )
  T:eq( R:getrange("foo",0,3), "This" )
  T:eq( R:getrange("foo",-3,-1), "ing" )
  T:eq( R:getrange("foo",0,-1), "This is a string" )
  T:eq( R:getrange("foo","5","-8"), "is a" )
  T:eq( R:getrange("foo",9,100000), " string" )
  T:eq( R:set("foo","Hello World!"), true )
  T:eq( R:setrange("foo",6,"Redis"), 12 )
  T:eq( R:get("foo"), "Hello Redis!" )
  T:eq( R:del("foo"), 1 )
  T:eq( R:setrange("foo",10,"bar"), 13 )
  T:eq( R:get("foo"), "\0\0\0\0\0\0\0\0\0\0bar" )
  T:eq( R:set("foo","bar"), true )
  T:eq( R:setrange("foo","1","A"), 3 )
  T:eq( R:get("foo"), "bAr" )
  T:eq( R:del("foo","chunky","spam"), 3 )
  T:rk_nil( "foo" )
  T:eq( R:incrby("foo",5), 5 )
  T:eq( R:incrby("foo","3"), 8 )
  T:eq( R:incr("foo"), 9 )
  T:eq( R:incr("foo"), 10 )
  T:err( function() R:incrby("foo",math.huge) end )
  T:eq( R:get("foo"), "10" )
  T:eq( R:decr("foo"), 9 )
  T:eq( R:decrby("foo",3), 6 )
  T:eq( R:decrby("foo",-2), 8 )
  T:eq( R:decrby("foo","1"), 7 )
  T:eq( R:decrby("foo","-1"), 8 )
  T:eq( R:set("foo","234293482390480948029348230948"), true )
  T:err( function() R:decr("foo") end )
  if TEST_REDIS_LUA then -- see https://github.com/nrk/redis-lua/issues/30
    T:eq( R:set("foo",string.format("%d",-(2^53) + 5)), true )
  else
    T:eq( R:set("foo",-(2^53) + 5), true )
  end
  T:eq( R:decr("foo"), -(2^53) + 4 )
  T:eq( R:incrby("foo",-1), -2^53+3 )
  if not TEST_REDIS_LUA then -- real Redis has higher limits
    T:err( function() R:incrby("foo",-10) end )
  end
  T:eq( R:set("foo",2), true )
  T:err( function() R:incrby("foo","234293482390480948029348230948") end )
  T:eq( R:set("foo",2), true )
  T:eq( R:incrby("foo",-5), -3 )
  T:eq( R:get("foo"), "-3" )
  T:err( function() R:incrby("foo",2.5) end )
  T:eq( R:incrbyfloat("foo",2.5), -0.5 )
  T:eq( R:set("foo","5.0e3"), true )
  T:eq( R:incrbyfloat("foo","2.0e2"), 5200 )
  T:err( function() R:incrbyfloat("foo","two.6") end )
  T:eq( R:get("foo"), "5200" )
  T:eq( R:del("foo"), 1 )
  T:rk_nil( "foo" ); T:rk_nil( "spam" ); T:rk_nil( "chunky" )
  T:rk_nil( "get" )
  T:eq( R:set("get","foo"), true )
  T:eq( R:get("get"), "foo")
  T:eq( R:del("get"), 1 )
  T:eq( R:get("get"), nil )
end; T:done()

--- bit operations

T:start("bit operations"); do
  T:eq( R:set("foo","foobar"), true )
  T:eq( R:bitcount("foo"), 26 )
  T:eq( R:bitcount("foo",0,0), 4 )
  T:eq( R:bitcount("foo",1,1), 6 )
  T:eq( R:bitcount("foo",0,1), 10 )
  T:eq( R:set("foo",""), true )
  T:eq( R:bitcount("foo"), 0 )
  T:eq( R:set("k1","foobar"), true )
  T:eq( R:set("k2","abcdef"), true )
  T:eq( R:bitop("and","foo","k1","k2"), 6 )
  T:eq( R:get("foo"), "`bc`ab" )
  T:eq( R:bitop("or","foo","k1","k2"), 6 )
  T:eq( R:get("foo"), "goofev" )
  T:eq( R:bitop("xor","foo","k1","k2"), 6 )
  T:eq( R:get("foo"), string.char(0x07,0x0d,0x0c,0x06,0x04,0x14) )
  T:eq( R:bitop("not","foo","k1"), 6 )
  T:eq( R:get("foo"), string.char(0x99,0x90,0x90,0x9d,0x9e,0x8d) )
  T:eq( R:bitop("not","foo","foo"), 6 )
  T:eq( R:get("foo"), "foobar" )
  T:eq( R:set("k2","abc"), true )
  T:eq( R:bitop("and","foo","k1","k2"), 6 )
  T:eq( R:get("foo"), "`bc\0\0\0" )
  T:eq( R:bitop("or","foo","k2","k1"), 6 )
  T:eq( R:get("foo"), "goobar" )
  T:eq( R:set("k1",""), true )
  T:eq( R:del("k2"), 1 )
  T:eq( R:bitop("not","foo","k1"), 0 )
  T:rk_nil( "foo" )
  T:eq( R:set("foo","bar"), true )
  T:eq( R:bitop("not","foo","k2"), 0 )
  T:rk_nil( "foo" )
  T:eq( R:setbit("foo",0,1), 0 )
  T:eq( R:getbit("foo",0), 1 )
  T:eq( R:setbit("foo",0,1), 1 )
  T:eq( R:getbit("foo",0), 1 )
  T:eq( R:setbit("foo",0,0), 1 )
  T:eq( R:getbit("foo",0), 0 )
  T:eq( R:setbit("foo",0,0), 0 )
  T:eq( R:get("foo"), "\0" )
  T:eq( R:setbit("foo",8,1), 0 )
  T:eq( R:setbit("foo",10,1), 0 )
  T:eq( R:setbit("foo",16,1), 0 )
  T:eq( R:getbit("foo",7), 0 )
  T:eq( R:getbit("foo",8), 1 )
  T:eq( R:getbit("foo",16), 1 )
  T:eq( R:getbit("foo",32), 0 )
  T:eq( R:get("foo"), string.char(0x00,0xa0,0x80) )
  T:eq( R:del("foo","k1","k2"), 2 )
  T:eq( R:bitcount("foo"), 0 )
  T:rk_nil( "foo" ); T:rk_nil( "k1" ); T:rk_nil( "k2" )
end; T:done()

--- hashes

T:start("hashes"); do
  T:rk_nil( "foo" )
  T:eq( R:hlen("foo"), 0 )
  T:eq( R:hget("foo","bar"), nil )
  T:eq( R:hgetall("foo"), {} )
  T:seq( R:hkeys("foo"), {} )
  T:seq( R:hvals("foo"), {} )
  T:rk_nil_hash( "foo","bar" )
  T:eq( R:hset("foo","spam","eggs"), true )
  T:eq( R:exists("foo"), true )
  T:eq( R:hexists("foo","spam"), true )
  T:eq( R:type("foo"), "hash" )
  T:eq( R:hget("foo","bar"), nil )
  T:eq( R:hset("foo","bar","baz"), true )
  T:eq( R:hget("foo","bar"), "baz" )
  T:eq( R:hlen("foo"), 2 )
  T:seq( R:hkeys("foo"), {"spam","bar"} )
  T:seq( R:hvals("foo"), {"eggs","baz"} )
  T:eq( R:hgetall("foo"), {spam="eggs",bar="baz"} )
  T:eq( R:hmget("foo",{"spam","trap","bar"}), {"eggs",nil,"baz"} )
  T:eq( R:hmset("foo",{bar="biz",chunky="bacon"}), true )
  T:eq( R:hgetall("foo"), {spam="eggs",bar="biz",chunky="bacon"} )
  T:eq( R:hmset("foo","bar","baz","yak","yak"), true )
  T:eq( R:hgetall("foo"), {spam="eggs",bar="baz",chunky="bacon",yak="yak"} )
  T:eq( R:hdel("foo","bar","yak"), 2 )
  T:rk_nil_hash( "foo","bar" )
  T:eq( R:hdel("foo","bar"), 0 )
  T:eq( R:hget("foo","spam"), "eggs" )
  T:eq( R:hsetnx("foo","spam","spam"), false )
  T:eq( R:hsetnx("foo","bar","baz"), true )
  T:eq( R:hgetall("foo"), {spam="eggs",bar="baz",chunky="bacon"} )
  T:eq( R:hset("foo","spam","eggs"), false )
  T:eq( R:hdel("foo","spam","bar"), 2 )
  T:eq( R:hdel("foo","bar"), 0 )
  T:eq( R:hdel("foo","chunky"), 1 )
  T:eq( R:del("foo"), 0 )
  T:rk_nil_hash( "foo","bar" )
  T:rk_nil( "foo" )
  T:eq( R:hincrby("foo","bar",5), 5 )
  T:eq( R:hincrby("foo","bar",3), 8 )
  T:eq( R:hincrby("foo","bar",-9), -1 )
  T:err( function() R:hincrby("foo","bar",-0.5) end )
  T:eq( R:hincrbyfloat("foo","bar",-0.5), -1.5 )
  T:eq( R:hincrbyfloat("foo","bar","-0.5e-1"), -1.55 )
  T:err( function() R:hincrbyfloat("foo","bar","6.two") end )
  T:eq( R:hget("foo","bar"), "-1.55" )
  T:eq( R:hset("foo","bar",string.format("%d",2^53-2)), false )
  if not TEST_REDIS_LUA then -- real Redis has higher limits
    T:err( function() R:hincrby("foo","bar",2) end )
  end
  T:eq( R:hincrby("foo","bar",1), 2^53-1 )
  T:eq( R:hincrby("foo","bar",-1), 2^53-2 )
  T:eq( R:hset("foo","bar",string.format("%d",2^53)), false )
  if not TEST_REDIS_LUA then -- real Redis has higher limits
    T:err( function() R:hincrby("foo","bar",-1) end )
  end
  T:eq( R:del("foo"), 1 )
  T:rk_nil( "foo" )
end; T:done()

--- lists

T:start("lists"); do
  T:rk_nil( "foo" )
  T:eq( R:llen("foo"), 0 )
  T:eq( R:lrange("foo",0,-1), {} )
  T:eq( R:lindex("foo",0), nil )
  T:eq( R:lindex("foo",-1), nil )
  T:eq( R:lpop("foo"), nil )
  T:eq( R:rpop("foo"), nil )
  T:eq( R:lpush("foo","A"), 1 )
  T:eq( R:llen("foo"), 1 )
  T:eq( R:lrange("foo",0,-1), {"A"} )
  T:eq( R:lindex("foo",0), "A" )
  T:eq( R:lindex("foo",-1), "A" )
  T:eq( R:lindex("foo",1), nil )
  T:eq( R:lindex("foo",-2), nil )
  T:eq( R:lpop("foo"), "A" )
  T:rk_nil( "foo" )
  T:eq( R:lpush("foo","A"), 1 )
  T:eq( R:rpop("foo"), "A" )
  T:rk_nil( "foo" )
  T:eq( R:rpush("foo","A"), 1 )
  T:eq( R:llen("foo"), 1 )
  T:eq( R:lrange("foo",0,-1), {"A"} )
  T:eq( R:lindex("foo",0), "A" )
  T:eq( R:lindex("foo",-1), "A" )
  T:eq( R:lindex("foo",1), nil )
  T:eq( R:lindex("foo",-2), nil )
  T:eq( R:rpop("foo"), "A" )
  T:rk_nil( "foo" )
  T:eq( R:rpush("foo","A"), 1 )
  T:eq( R:lpop("foo"), "A" )
  T:rk_nil( "foo" )
  T:eq( R:lpushx("foo","T"), 0 )
  T:rk_nil( "foo" )
  T:eq( R:lpush("foo","A"), 1 )
  T:eq( R:lpushx("foo","B"), 2 )
  T:eq( R:lpush("foo","C"), 3 )
  T:eq( R:llen("foo"), 3 )
  T:eq( R:lrange("foo",0,-1), {"C","B","A"} )
  T:eq( R:lrange("foo",0,0), {"C"} )
  T:eq( R:lrange("foo",0,1), {"C","B"} )
  T:eq( R:lrange("foo",0,2), {"C","B","A"} )
  T:eq( R:lrange("foo",1,2), {"B","A"} )
  T:eq( R:lrange("foo",2,2), {"A"} )
  T:eq( R:lrange("foo",2,0), {} )
  T:eq( R:lpop("foo"), "C" )
  T:eq( R:llen("foo"), 2 )
  T:eq( R:rpop("foo"), "A" )
  T:eq( R:llen("foo"), 1 )
  T:eq( R:rpush("foo","X"), 2 )
  T:eq( R:lpop("foo"), "B" )
  T:eq( R:lpop("foo"), "X" )
  T:eq( R:llen("foo"), 0 )
  T:eq( R:lpop("foo"), nil )
  T:rk_nil( "foo" )
  T:eq( R:rpushx("foo","T"), 0 )
  T:rk_nil( "foo" )
  T:eq( R:rpush("foo","A"), 1 )
  T:eq( R:rpushx("foo","B"), 2 )
  T:eq( R:rpush("foo","C"), 3 )
  T:eq( R:llen("foo"), 3 )
  T:eq( R:lrange("foo",0,-1), {"A","B","C"} )
  T:eq( R:lrange("foo",0,0), {"A"} )
  T:eq( R:lrange("foo",0,1), {"A","B"} )
  T:eq( R:lrange("foo",0,2), {"A","B","C"} )
  T:eq( R:lrange("foo",0,3), {"A","B","C"} )
  T:eq( R:lrange("foo",1,2), {"B","C"} )
  T:eq( R:lrange("foo",2,2), {"C"} )
  T:eq( R:lrange("foo",2,0), {} )
  T:eq( R:lrange("foo",-1,-2), {} )
  T:eq( R:lrange("foo",-1,-1), {"C"} )
  T:eq( R:lrange("foo",-2,-1), {"B","C"} )
  T:eq( R:lrange("foo",-3,-1), {"A","B","C"} )
  T:eq( R:lrange("foo",-4,-1), {"A","B","C"} )
  T:eq( R:lrange("foo","1","-2"), {"B"} )
  T:eq( R:lindex("foo",0), "A" )
  T:eq( R:lindex("foo",-3), "A" )
  T:eq( R:lindex("foo",1), "B" )
  T:eq( R:lindex("foo",-2), "B" )
  T:eq( R:lindex("foo","2"), "C" )
  T:eq( R:lindex("foo","-1"), "C" )
  T:eq( R:lindex("foo",3), nil )
  T:eq( R:lindex("foo",-4), nil )
  T:eq( R:rpop("foo"), "C" )
  T:eq( R:llen("foo"), 2 )
  T:eq( R:lpop("foo"), "A" )
  T:eq( R:llen("foo"), 1 )
  T:eq( R:lpush("foo","X"), 2 )
  T:eq( R:rpop("foo"), "B" )
  T:eq( R:rpop("foo"), "X" )
  T:eq( R:llen("foo"), 0 )
  T:eq( R:rpop("foo"), nil )
  T:rk_nil( "foo" )
  T:eq( R:lpush("foo","A","B","C"), 3 )
  T:eq( R:llen("foo"), 3 )
  T:eq( R:lrange("foo",0,-1), {"C","B","A"} )
  T:eq( R:del("foo"), 1 )
  T:rk_nil( "foo" )
  T:eq( R:rpush("foo","A","B","C"), 3 )
  T:eq( R:llen("foo"), 3 )
  T:eq( R:lrange("foo",0,-1), {"A","B","C"} )
  T:eq( R:lset("foo",0,"X"), true )
  T:eq( R:lset("foo","2","Z"), true )
  T:eq( R:lset("foo",1,"Y"), true )
  T:eq( R:lrange("foo",0,-1), {"X","Y","Z"} )
  T:err( function() R:lset("foo",3,"T") end )
  T:eq( R:lset("foo",-1,"C"), true )
  T:eq( R:lset("foo",-2,"B"), true )
  T:eq( R:lset("foo","-3","A"), true )
  T:err( function() R:lset("foo",-4,"T") end )
  T:eq( R:lrange("foo",0,-1), {"A","B","C"} )
  T:eq( R:ltrim("foo",0,-1), true )
  T:eq( R:lrange("foo",0,-1), {"A","B","C"} )
  T:eq( R:ltrim("foo",0,2), true )
  T:eq( R:lrange("foo",0,-1), {"A","B","C"} )
  T:eq( R:ltrim("foo",-3,2), true )
  T:eq( R:lrange("foo",0,-1), {"A","B","C"} )
  T:eq( R:ltrim("foo",-4,3), true )
  T:eq( R:lrange("foo",0,-1), {"A","B","C"} )
  T:eq( R:ltrim("foo",0,-2), true )
  T:eq( R:lrange("foo",0,-1), {"A","B"} )
  T:eq( R:rpush("foo","C"), 3 )
  T:eq( R:ltrim("foo",0,1), true )
  T:eq( R:lrange("foo",0,-1), {"A","B"} )
  T:eq( R:rpush("foo","C"), 3 )
  T:eq( R:ltrim("foo",0,0), true )
  T:eq( R:lrange("foo",0,-1), {"A"} )
  T:eq( R:rpush("foo","B","C"), 3 )
  T:eq( R:ltrim("foo",1,2), true )
  T:eq( R:lrange("foo",0,-1), {"B","C"} )
  T:eq( R:lpush("foo","A"), 3 )
  T:eq( R:ltrim("foo","-2","2"), true )
  T:eq( R:lrange("foo",0,-1), {"B","C"} )
  T:eq( R:lpush("foo","A"), 3 )
  T:eq( R:ltrim("foo",2,2), true )
  T:eq( R:lrange("foo",0,-1), {"C"} )
  T:eq( R:lpush("foo","B","A"), 3 )
  T:eq( R:ltrim("foo",3,4), true )
  T:rk_nil( "foo" )
  T:eq( R:rpush("foo","A","B","C"), 3 )
  T:eq( R:ltrim("foo",-4,-4), true )
  T:rk_nil( "foo" )
  T:eq( R:rpush("foo","A","B","C"), 3 )
  T:eq( R:ltrim("foo",2,1), true )
  T:rk_nil( "foo" )
  T:eq( R:rpush("foo","A","B","C"), 3 )
  T:eq( R:linsert("foo","before","B","X"), 4 )
  T:eq( R:linsert("foo","before","C","X"), 5 )
  T:eq( R:linsert("foo","before","A","X"), 6 )
  T:eq( R:linsert("foo","before","T","X"), -1 )
  T:eq( R:linsert("foo","before","X","Z"), 7 )
  T:eq( R:lrange("foo",0,-1), {"Z","X","A","X","B","X","C"} )
  T:eq( R:linsert("foo","after","X","Z"), 8 )
  T:eq( R:linsert("foo","after","C",4), 9 )
  T:eq( R:lrange("foo",0,-1), {"Z","X","Z","A","X","B","X","C","4"} )
  T:eq( R:lrem("foo",2,"X"), 2 )
  T:eq( R:lrange("foo",0,-1), {"Z","Z","A","B","X","C","4"} )
  T:eq( R:lrem("foo",0,"Z"), 2 )
  T:eq( R:lrange("foo",0,-1), {"A","B","X","C","4"} )
  T:eq( R:linsert("foo","before","A","X"), 6 )
  T:eq( R:linsert("foo","before","B","X"), 7 )
  T:eq( R:lrem("foo",-1,"X"), 1 )
  T:eq( R:lrange("foo",0,-1), {"X","A","X","B","C","4"} )
  T:eq( R:lrem("foo",0,"T"), 0 )
  T:eq( R:lrem("foo","5","X"), 2 )
  T:eq( R:lrange("foo",0,-1), {"A","B","C","4"} )
  T:eq( R:del("foo"), 1 )
  T:eq( R:linsert("foo","before","T","X"), 0 )
  T:rk_nil( "foo" )
  T:eq( R:rpush("k1","A"), 1 )
  T:eq( R:rpush("k2","A"), 1 )
  T:eq( R:rpush("k2","B","C"), 3 )
  T:eq( R:blpop("k1","k2","k3",0), {"k1","A"} )
  T:eq( R:blpop("k1","k2","k3",1), {"k2","A"} )
  T:eq( R:blpop("k1","k2","k3",0), {"k2","B"} )
  T:eq( R:blpop("k1","k2","k3",1), {"k2","C"} )
  T:rk_nil( "k1" ); T:rk_nil( "k2" ); T:rk_nil( "k3" )
  T:eq( R:blpop("k1","k2","k3",1), nil )
  if not TEST_REDIS_LUA then
    T:err(
      function() R:blpop("k1","k2","k3",0) end,
      "operation would block"
    )
  end
  T:eq( R:lpush("k1","A"), 1 )
  T:eq( R:lpush("k2","A"), 1 )
  T:eq( R:lpush("k2","B","C"), 3 )
  T:eq( R:brpop("k1","k2","k3",0), {"k1","A"} )
  T:eq( R:brpop("k1","k2","k3",1), {"k2","A"} )
  T:eq( R:brpop("k1","k2","k3",0), {"k2","B"} )
  T:eq( R:brpop("k1","k2","k3",1), {"k2","C"} )
  T:rk_nil( "k1" ); T:rk_nil( "k2" ); T:rk_nil( "k3" )
  T:eq( R:brpop("k1","k2","k3",1), nil )
  if not TEST_REDIS_LUA then
    T:err(
      function() R:brpop("k1","k2","k3",0) end,
      "operation would block"
    )
  end
  T:eq( R:rpush("k1","A","B","C"), 3 )
  T:eq( R:rpoplpush("k1","k1"), "C" )
  T:eq( R:lrange("k1",0,-1), {"C","A","B"} )
  T:eq( R:brpoplpush("k1","k1",0), "B" )
  T:eq( R:lrange("k1",0,-1), {"B","C","A"} )
  T:eq( R:rpoplpush("k2","k2"), nil )
  T:rk_nil( "k2" )
  T:eq( R:brpoplpush("k2","k1",1), nil )
  T:rk_nil( "k2" )
  T:eq( R:lrange("k1",0,-1), {"B","C","A"} )
  if not TEST_REDIS_LUA then
    T:err(
      function() R:brpoplpush("k2","k1",0) end,
      "operation would block"
    )
  end
  T:eq( R:rpoplpush("k1","k2"), "A" )
  T:eq( R:brpoplpush("k1","k3",0), "C" )
  T:eq( R:brpoplpush("k1","k2",1), "B" )
  T:eq( R:rpoplpush("k3","k2"), "C" )
  T:eq( R:lrange("k2",0,-1), {"C","B","A"} )
  T:eq( R:del("k2"), 1 )
  T:rk_nil( "k1" ); T:rk_nil( "k2" ); T:rk_nil( "k3" )
end; T:done()

--- sets

T:start("sets"); do
  T:rk_nil( "foo" )
  T:eq( R:scard("foo"), 0 )
  T:eq( R:sismember("foo","A"), false )
  T:seq( R:smembers("foo"), {} )
  T:eq( R:srandmember("foo"), nil )
  T:eq( R:srandmember("foo",1), {} )
  T:eq( R:srandmember("foo",-1), {} )
  T:eq( R:spop("foo"), nil )
  T:eq( R:sadd("foo","A"), 1 )
  T:eq( R:exists("foo"), true )
  T:eq( R:type("foo"), "set" )
  T:eq( R:scard("foo"), 1 )
  T:eq( R:srandmember("foo"), "A" )
  T:eq( R:spop("foo"), "A" )
  T:eq( R:spop("foo"), nil )
  T:rk_nil( "foo" )
  T:eq( R:sadd("foo","A"), 1 )
  T:eq( R:sadd("foo","B"), 1 )
  T:eq( R:scard("foo"), 2 )
  T:eq( R:sadd("foo","A","C","D"), 2 )
  T:eq( R:scard("foo"), 4 )
  T:seq( R:smembers("foo"), {"A","B","C","D"} )
  T:eq( R:sismember("foo","B"), true )
  T:eq( R:srem("foo","B"), 1 )
  T:eq( R:srem("foo","B"), 0 )
  T:eq( R:srem("foo","B","C","D","E"), 2 )
  T:eq( R:scard("foo"), 1 )
  T:seq( R:smembers("foo"), {"A"} )
  T:eq( R:sismember("foo","B"), false )
  T:eq( R:del("foo"), 1 )
  T:eq( R:sadd("foo","A","B"), 2 )
  T:eq( R:srem("foo","A","B"), 2 )
  T:eq( R:del("foo"), 0 )
  T:eq( R:sismember("foo","A"), false )
  T:seq( R:smembers("foo"), {} )
  T:rk_nil( "foo" )
  T:eq( R:sadd("S1","A","B","C","D","E"), 5 )
  T:eq( R:sadd("S2","A","B","F"), 3 )
  T:eq( R:sadd("S3","A","C","D"), 3 )
  T:seq( R:sdiff("S1","S2","S3"), {"E"} )
  T:seq( R:sinter("S1","S2","S3"), {"A"} )
  T:seq( R:sunion("S1","S2","S3"), {"A","B","C","D","E","F"} )
  T:eq( R:sdiffstore("S0","S1","S2","S3"), 1 )
  T:seq( R:smembers("S0"), {"E"} )
  T:eq( R:sinterstore("S0","S1","S2","S3"), 1 )
  T:seq( R:smembers("S0"), {"A"} )
  T:eq( R:sunionstore("S0","S1","S2","S3"), 6 )
  T:seq( R:smembers("S0"), {"A","B","C","D","E","F"} )
  local _cur = {A = true,B = true,C = true,D = true,E = true,F = true}
  local _x
  for i=1,6 do
    _x = R:srandmember("S0")
    T:eq( _cur[_x], true )
    _x = R:spop("S0")
    T:eq( _cur[_x], true )
    _cur[_x] = false
    T:eq( R:scard("S0"), 6-i )
  end
  T:rk_nil( "S0" )
  T:eq( R:sunionstore("S0","S1","S2","S3"), 6 )
  T:seq( R:smembers("S0"), {"A","B","C","D","E","F"} )
  _cur = {A = true,B = true,C = true,D = true,E = true,F = true}
  T:eq( R:srandmember("S0",0), {} )
  T:seq( R:srandmember("S0",6), {"A","B","C","D","E","F"} )
  T:seq( R:srandmember("S0",8), {"A","B","C","D","E","F"} )
  _x = R:srandmember("S0",-3)
  T:eq( #_x, 3 )
  for i=1,#_x do T:eq( _cur[_x[i]], true ) end
  _x = R:srandmember("S0",-8)
  T:eq( #_x, 8 )
  for i=1,#_x do T:eq( _cur[_x[i]], true ) end
  local n
  for t=1,10 do
    _cur = {A = true,B = true,C = true,D = true,E = true,F = true}
    n = math.random(1,5)
    _x = R:srandmember("S0",n)
    T:eq( #_x, n )
    for i=1,n do
      T:eq( _cur[_x[i]], true )
      _cur[_x[i]] = false
    end
  end
  T:eq( R:smove("S2","S3","F"), true )
  T:eq( R:smove("S2","S3","F"), false )
  T:seq( R:smembers("S2"), {"A","B"} )
  T:seq( R:smembers("S3"), {"A","C","D","F"} )
  T:eq( R:del("S0","S1","S2","S3"), 4 )
end; T:done()

--- zsets

T:start("zsets"); do
  T:rk_nil("foo")
  T:eq( R:zadd("foo",10,"A"), 1 )
  T:eq( R:zadd("foo",20,"B","30","C"), 2 )
  T:eq( R:zcard("foo"), 3 )
  T:eq( R:zrange("foo",0,-1), {"A","B","C"} )
  T:eq(
    R:zrange("foo",0,-1,{withscores=true}),
    {{"A",10},{"B",20},{"C",30}}
  )
  T:eq( R:zadd("foo",30,"A",30.5,"D"), 1 )
  if not TEST_REDIS_LUA then
    T:yes( R:dbg_zcoherence("foo") )
  end
  T:eq(
    R:zrange("foo",0,-1,{withscores=true}),
    {{"B",20},{"A",30},{"C",30},{"D",30.5}}
  )
  T:eq(
    R:zrange("foo",1,2,{withscores=true}),
    {{"A",30},{"C",30}}
  )
  T:eq( R:zrange("foo",0,2), {"B","A","C"} )
  T:eq( R:zrange("foo",1,3), {"A","C","D"} )
  T:eq(
    R:zrevrange("foo",0,-1,{withscores=true}),
    {{"D",30.5},{"C",30},{"A",30},{"B",20}}
  )
  T:eq(
    R:zrevrange("foo",1,2,{withscores=true}),
    {{"C",30},{"A",30}}
  )
  T:eq( R:zrevrange("foo",0,2), {"D","C","A"} )
  T:eq( R:zrevrange("foo",1,3), {"C","A","B"} )
  T:eq( R:zcard("foo"), 4 )
  T:eq( R:zscore("foo","D"), 30.5 )
  T:eq( R:zscore("foo","nothing"), nil )
  T:eq( R:zscore("nothing","D"), nil )
  T:rk_nil("nothing")
  T:eq( R:zincrby("foo",2.3,"A"), 32.3 )
  T:eq( R:zincrby("foo",-0.5,"D"), 30 )
  T:eq( R:zincrby("foo",-0.5,"E"), -0.5 )
  T:eq(
    R:zrange("foo",0,-1,{withscores=true}),
    {{"E",-0.5},{"B",20},{"C",30},{"D",30},{"A",32.3}}
  )
  T:eq( R:zrangebyscore("foo",20,30), {"B","C","D"} )
  T:eq( R:zrangebyscore("foo","(20",30), {"C","D"} )
  T:eq( R:zrangebyscore("foo","(20","+inf"), {"C","D","A"} )
  T:eq( R:zrangebyscore("foo","-inf",30), {"E","B","C","D"} )
  T:eq( R:zrangebyscore("foo",20,"(30","withscores"), {{"B",20}} )
  T:eq( R:zrangebyscore("foo",-5,40), {"E","B","C","D","A"} )
  T:eq( R:zrangebyscore("foo",-5,40,"limit",1,3), {"B","C","D"} )
  T:eq(
    R:zrangebyscore("foo",-5,40,{limit={4,9},withscores=true}),
    {{"A",32.3}}
  )
  T:eq( R:zrevrangebyscore("foo",30,"(20"), {"D","C"} )
  T:eq( R:zrevrangebyscore("foo",30,"(20"), {"D","C"} )
  T:eq( R:zrevrangebyscore("foo","(30",20,"withscores"), {{"B",20}} )
  T:eq( R:zrevrangebyscore("foo",40,-5), {"A","D","C","B","E"} )
  T:eq( R:zrevrangebyscore("foo",40,-5,"limit",1,3), {"D","C","B"} )
  T:eq(
    R:zrevrangebyscore("foo",40,-5,{limit={4,9},withscores=true}),
    {{"E",-0.5}}
  )
  T:eq( R:zcount("foo",-10,-5), 0 )
  T:eq( R:zcount("foo","(-10",50), 5 )
  T:eq( R:zcount("foo","(20",30), 2 )
  T:eq( R:zrank("foo","E"), 0 )
  T:eq( R:zrank("foo","B"), 1 )
  T:eq( R:zrank("foo","A"), 4 )
  T:eq( R:zrevrank("foo","E"), 4 )
  T:eq( R:zrevrank("foo","B"), 3 )
  T:eq( R:zrevrank("foo","A"), 0 )
  T:eq( R:zrank("nothing","E"), nil )
  T:eq( R:zrevrank("nothing","E"), nil )
  T:eq( R:zrank("foo","nothing"), nil )
  T:eq( R:zrevrank("foo","nothing"), nil )
  T:eq( R:zrem("foo","D","B","X"), 2 )
  T:eq(
    R:zrange("foo",0,-1,{withscores=true}),
    {{"E",-0.5},{"C",30},{"A",32.3}}
  )
  T:eq( R:zrem("foo","A","E"), 2 )
  T:eq(
    R:zrange("foo",0,-1,{withscores=true}),
    {{"C",30}}
  )
  T:eq( R:zrem("foo","A","C"), 1 )
  T:eq(
    R:zrange("foo",0,-1,{withscores=true}),
    {}
  )
  T:rk_nil("foo")
  T:eq( R:zadd("foo",10,"A",20,"B",30,"C",40,"D",50,"E"), 5 )
  T:eq( R:zremrangebyrank("foo",0,1), 2 )
  T:eq( R:zremrangebyrank("foo",1,4), 2 )
  T:eq( R:zrange("foo",0,-1), {"C"} )
  T:eq( R:zadd("foo",10,"A",20,"B",30,"C",40,"D",50,"E"), 4 )
  T:eq( R:zremrangebyscore("foo",0,"(10"), 0 )
  T:eq( R:zremrangebyscore("foo",0,"10"), 1 )
  T:eq( R:zremrangebyscore("foo","(20",40), 2 )
  T:eq( R:zrange("foo",0,-1), {"B","E"} )
  T:eq( R:zadd("foo",10,"A",20,"B",30,"C",40,"D",50,"E"), 3 )
  T:eq( R:zremrangebyrank("foo",-3,-2), 2 )
  T:eq( R:zrange("foo",0,-1), {"A","B","E"} )
  T:eq( R:zremrangebyrank("foo",0,-1), 3 )
  T:rk_nil("foo")
  T:eq( R:zadd("z1",10,"A",20,"B",30,"C"), 3 )
  T:eq( R:zadd("z2",3,"A",7,"X",11,"C"), 3 )
  T:eq( R:zunionstore("z3",2,"z1","z2"), 4 )
  T:eq(
    R:zrange("z3",0,-1,"withscores"),
    {{"X",7},{"A",13},{"B",20},{"C",41}}
  )
  T:eq( R:zunionstore("z3",3,"z1","z2","zxxx","weights",1,-3,2), 4 )
  T:eq(
    R:zrange("z3",0,-1,"withscores"),
    {{"X",-21},{"C",-3},{"A",1},{"B",20}}
  )
  T:eq( R:zadd("z3",30,"C"), 0 )
  T:eq( R:zunionstore("z3",3,"z1","z2","z3","aggregate","min"), 4 )
  T:eq(
    R:zrange("z3",0,-1,"withscores"),
    {{"X",-21},{"A",1},{"C",11},{"B",20}}
  )
  T:eq( R:zunionstore("z3",2,"z1","z2","aggregate","max","weights",1,3), 4 )
  T:eq(
    R:zrange("z3",0,-1,"withscores"),
    {{"A",10},{"B",20},{"X",21},{"C",33}}
  )
  T:eq( R:del("z1","z2","z3"), 3 )
  T:eq( R:zadd("z1",10,"A",20,"B",30,"C"), 3 )
  T:eq( R:zadd("z2",3,"A",7,"X",11,"C"), 3 )
  T:eq( R:zinterstore("z3",2,"z1","z2"), 2 )
  T:eq(
    R:zrange("z3",0,-1,"withscores"),
    {{"A",13},{"C",41}}
  )
  T:eq( R:zinterstore("z3",2,"z3","z2",{aggregate="max",weights={1,4}}), 2 )
  T:eq(
    R:zrange("z3",0,-1,"withscores"),
    {{"A",13},{"C",44}}
  )
  T:eq( R:del("z1","z2","z3"), 3 )
end; T:done()

--- server

T:start("server"); do
  T:eq( R:echo("foo"), "foo" )
  T:eq( R:ping(), true )
end; T:done()

--- remaining key commands

T:start("keys"); do
  -- 'keys' command
  local _ks = {
    "",
    "foo",
    "afoo",
    "bar",
    "some-key",
    "foo:1",
    "foo:1:bar",
    "foo:2:bar",
    "this%is-really:tw][sted",
  }
  local _cases = {
    "",{""},
    "notakey",{},
    "*",_ks,
    "???",{"foo","bar"},
    "foo:*:*",{"foo:1:bar","foo:2:bar"},
    "*f[a-z]o",{"foo","afoo"},
    "*s%is-rea[j-m]??:*",{"this%is-really:tw][sted"},
  }
  local _ks2 = {}
  for i=1,#_ks do
    _ks2[#_ks2+1] = _ks[i]
    _ks2[#_ks2+1] = "x"
  end
  T:eq( R:mset(unpack(_ks2)), true )
  for i=1,#_cases/2 do
    T:seq( R:keys(_cases[2*i-1]), _cases[2*i] )
  end
  -- 'randomkey' command
  local _ks_set = {}
  for i=1,#_ks do _ks_set[_ks[i]] = true end
  local _cur,_prev
  local _founddiff,_notakey = false,false
  for i=1,100 do
    _cur = R:randomkey()
    if not _ks_set[_cur] then _notakey = true end
    if _cur ~= _prev then _founddiff = true end
    _prev = _cur
  end
  T:eq( _notakey, false )
  T:eq( _founddiff, true )
end; T:done()
