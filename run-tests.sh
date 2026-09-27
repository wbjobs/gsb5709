#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

BUILD_DIR=build
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

find src tests -name '*.java' | sort > "$BUILD_DIR/sources.txt"

# -source/-target 8 keep the code honest: JDK 8 syntax and APIs only.
javac -source 8 -target 8 -Xlint:-options -d "$BUILD_DIR" @"$BUILD_DIR/sources.txt"

java -cp "$BUILD_DIR" AsyncTest
