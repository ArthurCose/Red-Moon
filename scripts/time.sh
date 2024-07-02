#!/bin/bash

# call from workspace root

cargo build -p red_moon_cli --release

echo "red_moon " $({ time ./target/release/red_moon_cli $*; } 2>&1 | grep real)
echo "lua      " $({ time lua $*; } 2>&1 | grep real)
