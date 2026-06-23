local t = { 1, 2, 3, 4, a = 1, b = 2 }

print("ipairs:")
for i, v in ipairs(t) do
  print(i, v)
end

print("\npairs:")
for k, v in pairs(t) do
  print(k, v)
end

print("\nselect:")
print(select(1, 1, 2, 3))
print(select(-1, 1, 2, 3))
print(select(5, 1, 2, 3))
print(select(2, 1, 2, 3))
print(select("#", 1, 2, 3))
