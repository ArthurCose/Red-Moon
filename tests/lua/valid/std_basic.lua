local t = { 1, 2, 3, 4, a = 1, b = 2 }

print("ipairs:")
for i, v in ipairs(t) do
  print(i, v)
end

print("\npairs:")
for k, v in pairs(t) do
  print(k, v)
end

print("\npairs first item removed:")
local function iterate_map(callback)
  -- generate map
  local key_list = { "a", "b", "c", "d" }
  local t = {}

  for i, k in ipairs(key_list) do
    t[k] = i
  end

  -- loop over it with pairs
  local visit_counts = {}
  for k, _ in pairs(t) do
    callback(t, k)

    visit_counts[k] = (visit_counts[k] or 0) + 1
  end

  for _, k in ipairs(key_list) do
    print("visit_counts[\"" .. k .. "\"] = " .. (visit_counts[k] or 0))
  end

  return t
end

local iterations = 0

iterate_map(function(t, k, v)
  if iterations == 1 then
    t[k] = nil
  end

  iterations = iterations + 1
end)

print(iterations) -- should be #key_list

-- this makes sure something like swap_remove isn't messing with the order
print("\npairs prev item removed:")
local iterations = 0
local prev_key

iterate_map(function(t, k)
  if prev_key and iterations == 1 then
    t[prev_key] = nil
  end

  prev_key = k
  iterations = iterations + 1
end)

print(iterations) -- should be #key_list

print("\nselect:")
print(select(1, 1, 2, 3))
print(select(-1, 1, 2, 3))
print(select(5, 1, 2, 3))
print(select(2, 1, 2, 3))
print(select("#", 1, 2, 3))
