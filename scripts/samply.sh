cargo build -p red_moon_cli --profile profiling
mkdir _flamegraphs
samply record -o _flamegraphs/profile.json ./target/profiling/red_moon_cli $*
