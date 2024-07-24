local co
co = coroutine.create(function(i)
  print("running:", coroutine.running())
  print("yieldable:", coroutine.isyieldable())
  print("status:", coroutine.status(co)) -- resumed

  while coroutine.yield(i) do
    i = i + 1
  end

  return "end"
end)

print("running:", coroutine.running())
print("yieldable:", coroutine.isyieldable())
print("status:", coroutine.status(co)) -- suspended
print(coroutine.resume(co, 1))         -- 1
print("status:", coroutine.status(co)) -- suspended
print(coroutine.resume(co, true))      -- 2
print(coroutine.resume(co, true))      -- 3
print(coroutine.resume(co, false))     -- end
print("status:", coroutine.status(co)) -- dead
print("running:", coroutine.running())

print("\nyield from inner call:")
co = coroutine.create(function()
  (function()
    print("a")
    coroutine.yield()
    print("b")
  end)()
  print("c")
end)
coroutine.resume(co)
print("yielded")
coroutine.resume(co)
