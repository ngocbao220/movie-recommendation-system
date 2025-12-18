#!/bin/bash

# Script kiểm tra prerequisites trước khi chạy docker compose

echo "🔍 Checking prerequisites for Docker setup..."
echo ""

ERRORS=0

# Check Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker not installed"
    ERRORS=$((ERRORS + 1))
else
    echo "✅ Docker installed: $(docker --version)"
fi

# Check Docker Compose
if ! command -v docker compose &> /dev/null; then
    echo "❌ Docker Compose not available"
    ERRORS=$((ERRORS + 1))
else
    echo "✅ Docker Compose available"
fi

# Check required files
echo ""
echo "📁 Checking required files..."

FILES=(
    "data/links.csv"
    "data/movies.csv"
    "data/ratings.csv"
    "checkpoints/model_1_rulesv3/rules.parquet"
    "checkpoints/model_2_als/metadata/_SUCCESS"
    "data/cache/user_recommendations.json"
)

for file in "${FILES[@]}"; do
    if [ -f "$file" ]; then
        echo "✅ $file"
    else
        echo "❌ $file (MISSING)"
        ERRORS=$((ERRORS + 1))
    fi
done

# Check ports
echo ""
echo "🔌 Checking ports availability..."

if lsof -Pi :3000 -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "❌ Port 3000 already in use"
    ERRORS=$((ERRORS + 1))
else
    echo "✅ Port 3000 available"
fi

if lsof -Pi :8001 -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "❌ Port 8001 already in use"
    ERRORS=$((ERRORS + 1))
else
    echo "✅ Port 8001 available"
fi

# Check disk space
echo ""
echo "💾 Checking disk space..."
AVAILABLE=$(df -h . | awk 'NR==2 {print $4}')
echo "Available space: $AVAILABLE"

# Summary
echo ""
echo "═══════════════════════════════════════"
if [ $ERRORS -eq 0 ]; then
    echo "✅ All checks passed! Ready to run:"
    echo ""
    echo "   docker compose up"
    echo ""
else
    echo "❌ Found $ERRORS error(s)"
    echo ""
    echo "Please fix the issues above before running docker compose."
    echo "See DOCKER_SETUP.md for detailed instructions."
fi
echo "═══════════════════════════════════════"

exit $ERRORS
