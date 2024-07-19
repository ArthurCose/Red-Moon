-- move _ENV to heap ahead of time
(function() return _ENV end)()

-- clear from previous tests
collectgarbage()

local original_count = collectgarbage("count")

-- controlled
do
  -- stop the gc to control when it runs
  collectgarbage("stop")

  local function test(name, create_data)
    -- clear from previous tests
    collectgarbage()

    local data = create_data()
    local count_a = collectgarbage("count")
    collectgarbage()
    local count_b = collectgarbage("count")
    assert(count_a == count_b)

    data = nil
    collectgarbage()
    count_b = collectgarbage("count")
    assert(count_a > count_b)

    print(name .. " passed")
  end

  test("empty table", function() return {} end)
  test("table with data", function() return { a = {}, {} } end)
  test("up values", function()
    local a = {}
    return function() return a end
  end)

  ---@diagnostic disable-next-line: cast-local-type
  test = nil
  collectgarbage()
  assert(collectgarbage("count") == original_count)

  collectgarbage("restart")
end

-- automatic
do
  collectgarbage()

  local new_count = collectgarbage("count")
  local last_count
  local t

  -- generate garbage until the gc runs
  repeat
    last_count = new_count
    t = {}
    t = nil
    new_count = collectgarbage("count")
  until new_count < last_count

  print("new table automatic collection passed")
end

do
  collectgarbage()

  local new_count = collectgarbage("count")
  local last_count
  local t = {}
  local garbage = {}
  garbage = nil

  -- generate garbage until the gc runs
  local i = 1
  repeat
    last_count = new_count
    t[i] = i
    i = i + 1
    new_count = collectgarbage("count")
  until new_count < last_count

  t = nil

  print("appending automatic collection passed")
end

do
  collectgarbage()

  local new_count = collectgarbage("count")
  local last_count
  local t = {}
  local garbage = {}
  garbage = nil

  -- generate garbage until the gc runs
  -- starting at 5 to force use of keys instead of list logic
  local i = 5
  repeat
    last_count = new_count
    t[i] = i
    i = i + 1
    new_count = collectgarbage("count")
  until new_count < last_count

  t = nil

  print("new keys automatic collection passed")
end

do
  collectgarbage()

  local new_count = collectgarbage("count")
  local last_count
  local f
  local a = 0

  -- generate garbage until the gc runs
  repeat
    last_count = new_count
    f = function() return a end
    f = nil
    new_count = collectgarbage("count")
  until new_count < last_count

  print("closures automatic collection passed")
end

do
  collectgarbage()

  local new_count = collectgarbage("count")
  local last_count
  local s = ""
  local a = "a"

  -- generate garbage until the gc runs
  repeat
    last_count = new_count
    s = s .. a
    new_count = collectgarbage("count")
  until new_count < last_count

  print("concatination automatic collection passed")
end

collectgarbage()
assert(collectgarbage("count") == original_count)
