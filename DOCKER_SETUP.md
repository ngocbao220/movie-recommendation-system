# 🐳 Docker Setup - Movie Recommendation System

## 📋 Prerequisites

- Docker Desktop installed (with Docker Compose)
- At least 8GB RAM available
- Ports 3000 (frontend) and 8001 (backend) available

## 🚀 Quick Start

### 1. Clone & Prepare Data

```bash
# Clone repository
git clone <your-repo>
cd movie-recommendation-system

# IMPORTANT: Ensure you have these folders/files
# (they are in .gitignore, so you need to download separately)
checkpoints/model_1_rulesv3/rules.parquet
checkpoints/model_2_als/              # Model checkpoint
data/links.csv
data/movies.csv
data/ratings.csv
data/cache/user_recommendations.json  # Pre-computed cache
```

### 2. Start Everything with One Command

```bash
docker compose up
```

**What happens:**
1. Backend container builds (installs Python + PySpark + FastAPI)
2. Frontend container builds (installs Node.js + React + Vite)
3. Backend starts on http://localhost:8001
4. Frontend starts on http://localhost:3000
5. Auto-reload enabled for development

### 3. Access the Application

- **Frontend**: http://localhost:3000
- **Backend API**: http://localhost:8001
- **API Docs**: http://localhost:8001/docs

### 4. Login

- User ID: `1`, `2`, `3`, ... (any number 1-10 for cached users)
- Password: `1234`

## 🛠️ Development Mode

Volumes are mounted for hot-reload:

```yaml
backend:
  volumes:
    - ./src:/app/src          # Backend code
    - ./models:/app/models    # ML models
    - ./data:/app/data        # Data files
    
frontend:
  volumes:
    - ./frontend/src:/app/src  # React components
```

**Edit files locally → Changes reflect immediately in containers!**

## 📊 Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     Docker Network                       │
│                                                          │
│  ┌─────────────────────┐      ┌────────────────────┐   │
│  │   Frontend          │      │   Backend          │   │
│  │   React + Vite      │─────▶│   FastAPI + Spark  │   │
│  │   Port: 3000        │      │   Port: 8001       │   │
│  └─────────────────────┘      └────────────────────┘   │
│                                         │               │
│                                         ▼               │
│                               ┌──────────────────┐      │
│                               │  Models & Data   │      │
│                               │  - Model 1 Rules │      │
│                               │  - Model 2 ALS   │      │
│                               │  - TMDB Cache    │      │
│                               └──────────────────┘      │
└─────────────────────────────────────────────────────────┘
```

## 🔧 Useful Commands

```bash
# Start in background
docker compose up -d

# View logs
docker compose logs -f
docker compose logs backend -f
docker compose logs frontend -f

# Restart specific service
docker compose restart backend
docker compose restart frontend

# Rebuild after dependency changes
docker compose up --build

# Stop everything
docker compose down

# Remove all data (including volumes)
docker compose down -v

# Access backend shell
docker exec -it movie-backend bash

# Access frontend shell
docker exec -it movie-frontend sh
```

## 📦 What's Included

### Backend Container
- **Base**: jupyter/all-spark-notebook (Spark + Python)
- **Dependencies**: FastAPI, PySpark, Pandas, httpx
- **ML Models**: 
  - Model 1: Association Rules (8637 rules)
  - Model 2: ALS Collaborative Filtering
- **Cache**: Pre-computed recommendations for users 1-10

### Frontend Container
- **Base**: node:20-alpine
- **Framework**: React 18 + Vite
- **UI**: Tailwind CSS, Framer Motion
- **Features**: Login, Search, Hero section, Movie details

## ⚠️ Troubleshooting

### Backend won't start

```bash
# Check logs
docker compose logs backend

# Common issues:
# 1. Missing checkpoints/ folder
# 2. Missing data/ files
# 3. Port 8001 already in use
```

### Frontend can't connect to backend

```bash
# Ensure backend is healthy
docker compose ps

# Check network
docker network inspect movie-recommendation-system_movie-network
```

### Out of memory

```bash
# Increase Docker Desktop memory to 8GB+
# Settings → Resources → Memory → 8GB
```

### Rebuild from scratch

```bash
docker compose down -v
docker compose build --no-cache
docker compose up
```

## 🎯 Production Deployment

For production, create separate Dockerfiles:

1. **Backend**: Multi-stage build with smaller base image
2. **Frontend**: Build static files and serve with nginx
3. **Use environment variables** for configuration
4. **Add reverse proxy** (nginx/traefik)

Example production compose:

```yaml
services:
  backend:
    image: your-registry/movie-backend:latest
    environment:
      - ENVIRONMENT=production
      - TMDB_API_KEY=${TMDB_API_KEY}
    
  frontend:
    image: your-registry/movie-frontend:latest
    
  nginx:
    image: nginx:alpine
    ports:
      - "80:80"
```

## 📝 Notes

- **Data persistence**: Mounted as volumes, changes persist
- **Model training**: Not included in Docker (train locally first)
- **Cache generation**: Run `scripts/precompute_user_recs.py` before building
- **TMDB API**: Free tier, rate limit 40 requests/10 seconds

## 🚀 Next Steps

1. Pre-compute cache for more users (currently 1-10)
2. Add Redis for API caching
3. Add nginx reverse proxy
4. Implement CI/CD with GitHub Actions
5. Deploy to AWS/Azure/GCP

---

**Ready to go!** Just run `docker compose up` 🎬
