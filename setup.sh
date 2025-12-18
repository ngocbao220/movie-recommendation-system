#!/bin/bash

# Quick setup script cho đồng đội clone repo lần đầu

echo "🎬 Movie Recommendation System - Setup Script"
echo "=============================================="
echo ""

# Check if already setup
if [ -d "checkpoints/model_2_als" ] && [ -f "data/cache/user_recommendations.json" ]; then
    echo "✅ Models đã được train! Có thể chạy ngay:"
    echo ""
    echo "   docker compose up"
    echo ""
    exit 0
fi

echo "⚠️  Models chưa được train. Bạn cần:"
echo ""
echo "Option 1: Download checkpoints từ Google Drive/Cloud"
echo "   → Extract vào thư mục gốc"
echo "   → Chạy: docker compose up"
echo ""
echo "Option 2: Train models locally (mất 15-30 phút)"
read -p "Bạn muốn train ngay bây giờ? (y/n): " choice

if [ "$choice" != "y" ]; then
    echo ""
    echo "OK! Hãy download checkpoints từ đồng đội hoặc train sau:"
    echo ""
    echo "   python models/model_2_als/train.py"
    echo "   python scripts/precompute_user_recs.py"
    echo ""
    exit 0
fi

echo ""
echo "🔧 Bắt đầu training..."
echo ""

# Check Python
if ! command -v python &> /dev/null; then
    echo "❌ Python not found. Please install Python 3.9+"
    exit 1
fi

# Install dependencies
echo "📦 Installing Python dependencies..."
pip install -q -r requirements.txt

# Train Model 2
echo ""
echo "🤖 Training Model 2 (ALS)... (10-20 phút)"
python models/model_2_als/train.py

if [ $? -ne 0 ]; then
    echo "❌ Training failed!"
    exit 1
fi

# Pre-compute cache
echo ""
echo "⚡ Pre-computing cache for users 1-10... (3-5 phút)"
python scripts/precompute_user_recs.py

if [ $? -ne 0 ]; then
    echo "❌ Cache generation failed!"
    exit 1
fi

echo ""
echo "✅ Setup hoàn tất!"
echo ""
echo "Giờ có thể chạy:"
echo ""
echo "   docker compose up"
echo ""
echo "Hoặc manual:"
echo ""
echo "   # Terminal 1 - Backend"
echo "   python src/ui/api.py"
echo ""
echo "   # Terminal 2 - Frontend"
echo "   cd frontend && npm install && npm run dev"
echo ""
