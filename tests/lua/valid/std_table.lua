local t

print("table.concat():")
print(table.concat({ 1, 2, 3, 4 }))
print(table.concat({ 1, 2, 3, 4 }, ", "))
print(table.concat({ 1, 2, 3, 4 }, ", ", 1, 3))
print(table.concat({ 1, 2, 3, 4 }, ", ", 3, 4))

print("\ntable.pack() and table.unpack():")
print(table.unpack(table.pack(1, 2, 3)))

print("\ntable.move() move left:")
t = { 1, 2, 3, 4, 5, 6, 7 }
table.move(t, 2, 5, 1)
print(table.unpack(t))

print("\ntable.move() move right:")
t = { 1, 2, 3, 4, 5, 6, 7 }
table.move(t, 2, 5, 3)
print(table.unpack(t))

print("\ntable.move() extending:")
t = { 1, 2, 3, 4, 5, 6, 7 }
table.move(t, 1, #t, #t)
print(table.unpack(t))

print("\ntable.remove():")
for i = 1, 4 do
  t = { 1, 2, 3 }
  print(table.remove(t, i))
  print(table.remove(t))
  print(table.unpack(t))
end

print("\ntable.insert(t, i):")
for i = 1, 4 do
  t = { 1, 2, 3 }
  table.insert(t, i)
  print(table.unpack(t))
end

print("\ntable.insert(t, i, nil):")
t = { 1, 2, 3 }
table.insert(t, 2, nil)
print(table.unpack(t))

print("\ntable.insert(t, i, v):")
for i = 1, 4 do
  t = { 1, 2, 3 }
  table.insert(t, i, 0)
  print(table.unpack(t))
end

print("\ntable.sort():")
t = {}
table.sort(t)
print(table.unpack(t))

t = { 1 }
table.sort(t)
print(table.unpack(t))

t = { 2, 1, 1 }
table.sort(t)
print(table.unpack(t))

t = { 2, 1 }
table.sort(t)
print(table.unpack(t))

t = { 1, 2 }
table.sort(t)
print(table.unpack(t))

t = { 3, 1, 2, 5, 4 }
table.sort(t)
print(table.unpack(t))

t = { 1, 2, 3 }
table.sort(t, function(a, b)
  return a < b
end)
print(table.unpack(t))

t = { 1, 2, 3 }
table.sort(t, function(a, b)
  return a > b
end)
print(table.unpack(t))
