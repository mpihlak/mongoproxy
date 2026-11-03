#!/bin/sh
# Prints the mongoproxy version from proxy/Cargo.toml, at the given git ref or in the working tree.
set -e
if [ -n "$1" ]; then
  toml=$(git show "$1:proxy/Cargo.toml")
else
  toml=$(cat proxy/Cargo.toml)
fi
version=$(echo "$toml" | grep -m1 '^version' | cut -d'"' -f2)
if [ -z "$version" ]; then
  echo "No version found in proxy/Cargo.toml" >&2
  exit 1
fi
echo "$version"
