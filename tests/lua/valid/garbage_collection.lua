-- move _ENV to heap ahead of time
(function() return _ENV end)()

-- clear from previous tests
collectgarbage()

local original_count = collectgarbage("count")

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

test = nil
collectgarbage()
assert(collectgarbage("count") == original_count)
