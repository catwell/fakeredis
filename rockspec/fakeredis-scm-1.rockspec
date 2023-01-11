rockspec_format = "3.0"

package = "fakeredis"
version = "scm-1"

source = {
    url = "git://github.com/catwell/fakeredis.git",
}

description = {
    summary = "Redis mock",
    detailed = [[
        fakeredis is a Redis mock for Lua
        with the same interface as redis-lua.
    ]],
    homepage = "http://github.com/catwell/fakeredis",
    license = "MIT/X11",
}

dependencies = {
    "lua >= 5.1",
    -- "luasocket", -- only for blocking list methods with timeouts
    -- "LuaBitOp", -- only for Lua 5.1 (not LuaJIT, not 5.2)
}

build = {
    type = "none",
    install = { lua = { fakeredis = "fakeredis.lua" } },
    copy_directories = {},
}

test_dependencies = {
    "cwtest",
    "luasocket",
}

test = {
   type = "command",
   script = "fakeredis.test.lua",
}
