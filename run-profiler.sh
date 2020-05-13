#!/bin/bash

export PATH=$HOME/src/FlameGraph:$PATH

echo "Starting dtrace"
echo "Run the workload in another terminal, then come back here and ^C the dtrace."

stacks=tmp/out.stacks
rm -f $stacks
sudo dtrace -o $stacks -n 'profile-997 /execname == "mongoproxy"/ { @[ustack(100)] = count(); }' \
    -c 'target/release/mongoproxy --proxy 27111:127.0.0.1:27017'

echo "Generating flamegraph"
OUTPUT=tmp/prof.svg

stackcollapse.pl $stacks | flamegraph.pl > $OUTPUT
echo "Done, output in $OUTPUT"
