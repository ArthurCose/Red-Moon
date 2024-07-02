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

print("\nnumeric for loop:")
for i = 1, 5 do
  print(i)
end


print("\nnumeric for loop break:")
for i = 1, 5 do
  print(i)

  if i == 3 then
    break
  end
end

print("\nnumeric for loop reverse:")
for i = 5, 1, -1 do
  print(i)
end

print("\nnumeric for loop step:")
for i = 1, 10, 2 do
  print(i)
end

print("\nnumeric for loop scope:")
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
