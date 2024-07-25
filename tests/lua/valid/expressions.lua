print("values:")
print "hello"
print("hi", 'hello', [[world]], true, 1.0, 2, false, 0xFF, nil)

print("\nshort circuit:")
print(false or 1, 2 or false, true and 3, false and 4, true and false)

print("\nunary:")
print(not false and 3)

print("\narithmetic:")
print(1 + 2 * 2, 3 * 2 + 1, (1 + 2) * 2, 1. + 2, 1. << 2, 2 * -3, 2 ^ 2 ^ -1)

print("\nboolean comparison:")
print(1. == 1, 1. ~= 1, 1 < 2, 1 > 2, 1 >= 2, 1 <= 2, 1 >= 1, 1 <= 1)

print("\nstring comparison:")
print("a" < "b", "a" > "b", "a" == "b", "a" == "a", "a" >= "b", "a" >= "a")

print("\nstring arithmetic:")
print("1" + "1", "1" + "1.0", "1" + 1, "1" + 1.0, "1.0" + 1)

print("\nfunction comparison:")
local foo = function() end
local bar = function() end
print(foo == foo, foo ~= foo, foo == bar)

print("\nconcat:")
print("a" .. "b")
print("a" .. 1)
print((1) .. "b")

print("\nlength:")
print(#"1234")
print(#{ 1, 2, 3, 4, 5 })
