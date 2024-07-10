cargo build -p red_moon_cli --profile profiling
mkdir _profiling
samply record -o _profiling/profile.json ./target/profiling/red_moon_cli $*
