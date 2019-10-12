#!/bin/bash

export PATH=$HOME/src/FlameGraph:$PATH

echo "Starting dtrace"
echo "Run the workload in another terminal, then come back here and ^C the dtrace."

sudo dtrace -o tmp/out.stacks -n 'profile-997 /execname == "mongoproxy"/ { @[ustack(100)] = count(); }' \
    -c 'target/debug/mongoproxy --hostport 127.0.0.1:27017'

echo "Generating flamegraph"
OUTPUT=tmp/prof.svg

stackcollapse.pl tmp/out.stacks | flamegraph.pl > $OUTPUT
echo "Done, output in $OUTPUT"
