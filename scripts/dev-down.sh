#!/bin/bash

# Move to project root
cd "$(dirname "$0")/.." || exit 1

echo "🛑 Stopping and removing Docker containers..."
docker-compose -f docker/docker-compose.yml down "$@"

echo "✅ Done."
