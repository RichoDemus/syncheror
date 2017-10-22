#!/usr/bin/env bash
set -e

export CI=true

./gradlew check build buildImage

echo "Syncheror built successfully!"
