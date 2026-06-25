#!/bin/bash

# call from workspace root

cargo build -p red_moon_repl --profile release

MEASUREMENTS=30

hyperfine --warmup 50 --min-runs $MEASUREMENTS\
  "./target/release/red_moon_repl $*"\
  "lua $*"
