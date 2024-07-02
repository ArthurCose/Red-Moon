print("shadowing and scope exit:")
do
  local x = 1
  local executed = false

  ::test::

  -- this should print 1 twice
  print(x)

  if executed then
    goto test_end
  end

  executed = true

  -- should be stored as a separate local, avoiding writing 2 into the other x variable
  local x = 2

  goto test
  ::test_end::
end

print("\nskip local:")
do
  do
    goto test

    local a = "a"
    print(a)

    ::test::
  end
  print("b")
end

print("\nskip scope:")
do
  goto test

  do
    local a = "a"
    print(a)
  end

  ::test::

  print("b")
end
