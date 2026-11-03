#!/bin/sh
# Prints the files changed in the given git diff range that need a version bump.
git diff --name-only "$@" | grep -vE '\.md$|^\.github/|^img/|^examples/|^testing/|^tests/|^LICENSE$|^TODO\.txt$' || true
