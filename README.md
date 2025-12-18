# 🎬 Movie Recommendation System

Hệ thống gợi ý phim sử dụng Machine Learning với React frontend và FastAPI backend.

## ✨ Features

- 🤖 2 ML Models: Association Rules + ALS Collaborative Filtering
- ⚡ Pre-computed cache cho tốc độ cao
- 🎨 Modern UI với React + Tailwind CSS
- 🔍 Tìm kiếm phim với autocomplete
- 🎥 TMDB integration (posters, trailers)

## 🚀 Quick Start
chạy 2 phần này trước 

python models/model_2_als/train.py
python scripts/precompute_user_recs.py
xong thì chạy docker

### Chạy với Docker (Recommended)

```bash
docker compose up
```

Mở browser:
- Frontend: http://localhost:3000
- Backend: http://localhost:8001

Login với User ID: `1-10`, Password: `1234`

### Manual Setup (Không dùng Docker)

**Backend:**
```bash
pip install -r requirements.txt
python src/ui/api.py  # Port 8001
```

**Frontend:**
```bash
cd frontend
npm install
npm run dev  # Port 3000
```

## 📊 Tech Stack

- **Frontend**: React 18 + Vite + Tailwind CSS
- **Backend**: FastAPI + PySpark
- **ML**: Association Rules + ALS Collaborative Filtering
- **Data**: MovieLens + TMDB API

## 📁 Project Structure

```
├── src/ui/api.py           # Backend API
├── frontend/src/           # React components
├── models/                 # ML models
├── checkpoints/            # Trained models
└── data/                   # Dataset & cache
```

## ⚠️ Important Notes

- Models cần được **train trước** khi chạy:
  ```bash
  python models/model_2_als/train.py
  python scripts/precompute_user_recs.py
  ```
- Folders `data/` và `checkpoints/` bị gitignore (quá lớn)
- Download từ team hoặc train local

## 📖 More Info

- Docker setup: [DOCKER_SETUP.md](DOCKER_SETUP.md)
- Model 2 docs: [MODEL_2_ALS_DOCUMENTATION.txt](MODEL_2_ALS_DOCUMENTATION.txt)