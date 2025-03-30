#!/bin/bash

# Move to project root directory (assumes script is in ./scripts)
cd "$(dirname "$0")/.." || exit 1

echo "📁 Project root: $(pwd)"

# Check .env exists
if [ ! -f .env ]; then
  echo "❌ Error: .env file not found in project root!"
  echo "💡 Create one by copying .env.dev or .env.example"
  exit 1
fi

# Check GCP credentials file exists
CRED_PATH=$(grep GOOGLE_APPLICATION_CREDENTIALS .env | cut -d '=' -f2)
CRED_PATH=${CRED_PATH//\"/}  # remove quotes if any

if [ ! -f ".$CRED_PATH" ]; then
  echo "❌ Error: GCP credentials file not found at .$CRED_PATH"
  echo "💡 Make sure the file exists and the path in .env is correct"
  exit 1
fi

echo "✅ .env and credentials file found."
echo "🚀 Building Docker containers..."

docker-compose -f docker/docker-compose.yml up --build
