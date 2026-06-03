#!/bin/bash

set -e

PROJECT_DIR="$(pwd)"
INGESTION_DIR="$PROJECT_DIR/ingestion"

echo "=================================="
echo "🚀 GreenDevOps Bootstrap Script"
echo "=================================="

# ---------------------------
# 1. Check Go installation
# ---------------------------
if ! command -v go &> /dev/null; then
    echo "📦 Go not found → installing..."

    cd /tmp
    wget https://go.dev/dl/go1.22.5.linux-amd64.tar.gz
    sudo rm -rf /usr/local/go
    sudo tar -C /usr/local -xzf go1.22.5.linux-amd64.tar.gz

    echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
    export PATH=$PATH:/usr/local/go/bin

    echo "✅ Go installed"
else
    echo "✅ Go already installed"
fi

# ---------------------------
# 2. Go module fix
# ---------------------------
echo "📦 Fixing Go modules..."

cd "$INGESTION_DIR"

go mod tidy
go mod download

echo "✅ go.mod / go.sum ready"

# ---------------------------
# 3. Verify build
# ---------------------------
echo "🔨 Building Go binary..."

go build -o ingestion .

echo "✅ Go build successful"

# ---------------------------
# 4. Docker build
# ---------------------------
echo "🐳 Building Docker image..."

cd "$PROJECT_DIR"

docker build --no-cache -t greendevops-ingestion -f ingestion/Dockerfile ingestion/

echo "=================================="
echo "🎉 ALL DONE SUCCESSFULLY"
echo "=================================="
