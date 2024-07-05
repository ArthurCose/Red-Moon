#!/bin/bash

# call from workspace root

mkdir _flamegraphs
cargo flamegraph -p red_moon_cli --profile profiling --output="_flamegraphs/$(basename $1).svg" -- $*
rm perf.data
