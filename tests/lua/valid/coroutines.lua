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

print("\nyield from pcall:")
co = coroutine.create(function()
  pcall(function()
    print("a")
    coroutine.yield()
    print("b")
  end)
  print("c")
end)
coroutine.resume(co)
coroutine.resume(co)
coroutine.resume(co)

co = coroutine.create(function()
  local success = pcall(function()
    coroutine.yield()
    error()
  end)

  print("pcall result:", success)
end)
coroutine.resume(co)
coroutine.resume(co)

print("\nmultiple boundaries:")
co = coroutine.create(function()
  pcall(function()
    pcall(function()
      coroutine.yield()
      print("a")
    end)
    print("b")
  end)
  print("c")
end)
coroutine.resume(co)
coroutine.resume(co)

print("\nerror in multiple boundaries:")
co = coroutine.create(function()
  local success = pcall(function()
    local success = pcall(function()
      coroutine.yield()
      error()
    end)
    print("success a:", success)
    error()
  end)
  print("success b:", success)
end)
coroutine.resume(co)
coroutine.resume(co)
