local function implement_custom(table, name)
  table[name] = function(...) print(name, " called with args: ", ...) end
end

local m = {}
implement_custom(m, "__unm")
implement_custom(m, "__bnot")
implement_custom(m, "__add")
implement_custom(m, "__sub")
implement_custom(m, "__mul")
implement_custom(m, "__div")
implement_custom(m, "__idiv")
implement_custom(m, "__mod")
implement_custom(m, "__pow")
implement_custom(m, "__band")
implement_custom(m, "__bor")
implement_custom(m, "__bxor")
implement_custom(m, "__shl")
implement_custom(m, "__shr")
implement_custom(m, "__eq")
implement_custom(m, "__lt")
implement_custom(m, "__le")
implement_custom(m, "__concat")
implement_custom(m, "__len")
implement_custom(m, "__index")
implement_custom(m, "__newindex")
implement_custom(m, "__call")

local a = {}
setmetatable(a, m)

local noop = function(...) end

print("tables:")
noop(#a)
noop(-a)
noop(~a)
noop(a + 1, 1 + a)
noop(a - 1, 1 - a)
noop(a * 1, 1 * a)
noop(a / 1, 1 / a)
noop(a // 1, 1 // a)
noop(a % 1, 1 % a)
noop(a ^ 1, 1 ^ a)
noop(a & 1, 1 & a)
noop(a | 1, 1 | a)
noop(a ~ 1, 1 ~ a)
noop(a << 1, 1 << a)
noop(a >> 1, 1 >> a)
noop(a == 1, 1 == a)
noop(a ~= 1, 1 ~= a)
noop(a < 1, 1 < a)
noop(a > 1, 1 > a)
noop(a <= 1, 1 <= a)
noop(a >= 1, 1 >= a)
noop(a[1])
a[1] = 2
a(1, 2)

print("\n__index short circuit:")
local b = { value = 5 }
print(b.value)
setmetatable(b, m)
print(b.value)

print("\nsimple __index chain")
do
  local a, b, c = {}, {}, { value = 1 }
  setmetatable(a, { __index = b })
  setmetatable(b, { __index = c })
  print(a.value)
end
