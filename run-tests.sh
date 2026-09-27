#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

OUT=out
rm -rf "$OUT"
mkdir -p "$OUT"

echo "== compiling (JDK 8 syntax, --release 8) =="
javac --release 8 -Xlint:all -d "$OUT" $(find src test -name '*.java' | sort)

echo "== running tests =="
java -cp "$OUT" com.gsb.async.AsyncTest

echo "ALL TESTS PASSED"
