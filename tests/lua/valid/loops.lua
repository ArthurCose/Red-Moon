print("while loop:")
i = 0
while i < 5 do
  i = i + 1
  print(i)
end

print("\nwhile loop break:")
i = 0
while true do
  i = i + 1
  print(i)

  if i == 3 then
    break
  end
end

print("\nrepeat loop:")
i = 0
repeat
  i = i + 1
  print(i)
until i == 5

print("\nrepeat loop minimal:")
repeat
  print("ran once")
until true


print("\nrepeat loop break:")
i = 0
repeat
  i = i + 1
  print(i)

  if i == 3 then
    break
  end
until false

-- setup for later scope test
i = 0

print("\ninteger for loop:")
for i = 1, 5 do
  print(i)
end


print("\ninteger for loop break:")
for i = 1, 5 do
  print(i)

  if i == 3 then
    break
  end
end

print("\ninteger for loop reverse:")
for i = 5, 1, -1 do
  print(i)
end

print("\ninteger for loop step:")
for i = 1, 10, 2 do
  print(i)
end

print("\ninteger for loop scope:")
print(i)

-- setup for later scope test
i = 0

print("\nfloat for loop:")
for i = 1, 5.0 do
  print(i)
end

print("\nfloat for loop reverse:")
for i = 5.0, 1, -1 do
  print(i)
end

print("\nfloat for loop step:")
for i = 1, 5, 0.5 do
  print(i)
end

print("\nfloat for loop scope:")
print(i)

print("\ngeneric for loop:")
local function foo(limit, value)
  value = value + 1
  if value <= limit then
    return value
  end
end

for a in foo, 5, 0 do
  print(a)
end

print("\ngeneric for loop expression:")
local function bar()
  return foo, -1, -6
end

for a in bar() do
  print(a)
end

print("\ngeneric for loop break:")
for a in foo, 5, 0 do
  print(a)
  if a == 3 then
    break
  end
end

print("\ngeneric for loop simple:")
local function iter(max)
  local i = 0

  return function()
    if i >= max then
      return
    end

    i = i + 1

    return i
  end
end

for a in iter(5) do
  print(a)
end

print("\nnested break:")
for i = 1, 10 do
  print(i)
  -- making sure we skip the outer loop and not the inner loop
  break

  for i = 1, 10 do
  end
end

for i = 1, 3 do
  print(i)

  for i = 1, 10 do
    -- making sure we skip the inner loop and not the outer loop
    break
    print("running inner loop?")
  end
end

print("\nnested function break:")
for i = 1, 10, 1 do
  print(i)
  break

  local _ = function()
    for i = 1, 10, 1 do

    end

    print("???")
  end
end

print("\ngeneric for loop tail call:")
local function identity(value)
  return value
end

local function iterate(max_i)
  local i = 0

  return function()
    i = i + 1

    if i <= max_i then
      -- tail call optimization replaces the current function on the stack
      -- which our for loop wants to reuse
      return identity(max_i)
    end
  end
end

local iterations = 0
local max_iterations = 3

for _ in iterate(max_iterations) do
  if iterations >= max_iterations then
    error("should only iterate " .. max_iterations .. "x")
  end

  iterations = iterations + 1
end

-- print(iterations, iterate()())
if iterations == max_iterations then
  print("success")
end

print("\ngeneric for loop, control variable:")
iterate = function(invariant, control)
  if control >= invariant then
    return
  end

  control = control + 1

  return control
end

for i in iterate, 5, 0 do
  print(i)
  -- make sure we can't modify the control variable
  i = i + 1
end
