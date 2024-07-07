#!/bin/bash

# call from workspace root

cargo build -p red_moon_cli --release

MEASUREMENTS=10

time_multiple ()
{
  # first arg is a name
  # second arg is the executable
  # third arg and beyond is passed through
  for i in $(seq 1 $MEASUREMENTS)
    do echo "$1" $({ time $2 ${@:3}; } 2>&1 | grep real)
  done
}

time_multiple "red_moon " ./target/release/red_moon_cli $@
time_multiple "lua      " lua $@
