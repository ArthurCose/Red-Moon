print("functions:")
local function add(a, b)
  return a + b
end
print(add(1, 2))

local function multivalue()
  return 1, 2, 3, 4
end
print(add(8, 0), multivalue())
print(multivalue(), add(8, 0))

print("\nclosure curry:")
local function addCurry(a)
  return function(b)
    return a + b
  end
end
local curried = addCurry(13)
print(curried(15))
print(curried(16))
print(addCurry(13)(17))

print("\nclosure scopes:")
local i = 0
local function inc()
  do
    local i = 1
  end

  i = i + 1

  do
    local i = 1
  end

  return i
end

print(inc())
print(inc())
print(inc())
print(i)

print("\nclosure scopes 2:")
i = 0
local function inc2()
  do
    i = i + 1
  end

  return i
end

print(inc2())
print(inc2())
print(inc2())
print(i)

print("\nconditions:")
local function foo(a)
  local result
  if a == 0 then
    result = "a"
  elseif a == 1 then
    result = "b"
  else
    result = "c"
  end

  return result
end
print(foo(0))
print(foo(1))
print(foo(2))

print("\nvariadic:")
local function foo(...)
  return ...
end
print(foo(1, 2, 3))
local a, b, c = foo("a", "b", "c")
print(a, b, c)
local function foo(...)
  return 4, ...
end
print(foo(1, 2, 3))
local function foo(...)
  return ..., ...
end
print(foo(1, 2, 3))
local function foo(...)
  return ... - 2
end
print(foo(1, 2, 3))
local function foo(...)
  local a, b, c = ...
  return a, b, c
end
print(foo(1, 2, 3))
local function foo(a, ...)
  local a, b, c = ...
  return a, b, c
end
print(foo(1, 2, 3))

local function foo(a, ...)
  local a, b, c = 5, ...
  return a, b, c
end
print(foo(1, 2, 3))

print("\ntail calls")
function foo(n, max)
  if n >= max then
    return n
  end

  print(n)

  return foo(n + 1, max)
end

print(foo(1, 5))


print("\nempty return")
function foo()
  print(1 + 1)
  return
end

print(foo())


print("\nrecursion:")
local function recursive(a)
  if a > 5 then
    return a
  end
  return recursive(a + 1)
end
print(recursive(1))

print("\nmethod:")
local object = {}
function object:method(a, b)
  print(type(self), a, b)
end

object:method(2, 3)

print("\nfunction property:")
function object.function_property(a, b)
  print(a, b)
end

object.function_property(1, 2)
