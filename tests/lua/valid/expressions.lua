print("values:")
print "hello"
print("hi", 'hello', [[world]], true, 1.0, 2, false, 0xFF, nil)

print("\nshort circuit:")
print(false or 1, 2 or false, true and 3, false and 4, true and false)

print("\nshort_circuit_movement:")
local function short_circuit_movement(a, b, c)
  -- we've previously returned nil here while still passing tests
  -- from not copying to the right destinations
  print(c or 2)
end
short_circuit_movement(nil, nil, 1)

local a = 1
print(nil or a)

print("\nunary:")
print(not false and 3)

print("\narithmetic:")
print(1 + 2 * 2, 3 * 2 + 1, (1 + 2) * 2, 1. + 2, 1. << 2, 2 * -3, 2 ^ 2 ^ -1)

print("\nboolean comparison:")
print(1. == 1, 1. ~= 1, 1 < 2, 1 > 2, 1 >= 2, 1 <= 2, 1 >= 1, 1 <= 1)

print("\nbitwise and:")
print(1 & 1) -- 1, last bit was 1 in both
print(1 & 2) -- 0, no bits were 1 in the same column
print(1 & 3) -- 1, the last bit was 1 in both
print(3 & 2) -- 2, the second to last bit was 1 in both
print(3 & 7) -- 3, every bit in 3 matched against a bit in 7

print("\nbitwise or:")
print(1 | 2) -- 3, enabled the last two bits
print(1 | 3) -- 3, the matching bit was already on in 3
print(3 | 2) -- 3, the matching bit was already on in 3

print("\nbitwise not:")
print(~0) -- inverts bits, every bit is on in this number
print(~1) -- every bit except the last bit is on

print("\nbitwise xor:")
print(3 ~ 2) -- 1, the matching bit was disabled and preserved the rest
print(3 ~ 1) -- 2, the matching bit was disabled and preserved the rest
print(7 ~ 3) -- 4, disabled matching bits and preserved the rest
print(3 ~ 7) -- 4, disabled matching bits and preserved the rest

print("\nbitwise and not:")
print(7 & ~1) -- 6, inverted 1 so only other bits could pass / every bit in 1 was disabled
print(7 & ~3) -- 4, inverted 3 so only other bits could pass / every bit in 3 was disabled

print("\nbitwise bitshift:")
print(6 >> 1) -- 3, shifted bits to the right by 1: 0110 -> 0011
print(4 >> 2) -- 1, shifted bits to the right by 2: 0110 -> 0001
print(1 << 1) -- 2, shifted bits to the left by 1: 0001 -> 0010

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

print("\ncaptured operands:")
local n = 1
local b = false

local _ = function()
  return n + b
end

print(b or 1, b and true, n + 1)
