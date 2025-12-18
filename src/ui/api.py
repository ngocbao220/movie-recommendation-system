from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

import pandas as pd
import os
import httpx
import asyncio
import sys
import json

# Thư mục chứa api.py → src/ui
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Lùi 2 cấp: ui → src → project root
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))

# Thêm project root vào sys.path để import models
sys.path.insert(0, PROJECT_ROOT)

# data/ nằm ở project root - Sử dụng links.csv để lấy tmdbId
DATA_PATH = os.path.join(PROJECT_ROOT, "data", "links.csv")
MOVIES_PATH = os.path.join(PROJECT_ROOT, "data", "movies.csv")
CACHE_PATH = os.path.join(PROJECT_ROOT, "data", "cache", "user_recommendations.json")

df_map = pd.read_csv(DATA_PATH)
# Bỏ các dòng có tmdbId = NaN
df_map = df_map.dropna(subset=['tmdbId'])
df_map["tmdbId"] = df_map["tmdbId"].astype(int)
movieId_to_tmdb = dict(zip(df_map["movieId"], df_map["tmdbId"]))
tmdb_to_movieId = dict(zip(df_map["tmdbId"], df_map["movieId"]))  # Reverse mapping

# Load movies.csv để mapping movieId -> title
df_movies = pd.read_csv(MOVIES_PATH)
movieId_to_title = dict(zip(df_movies["movieId"], df_movies["title"]))
title_to_movieId = {v: k for k, v in movieId_to_title.items()}

# Import Model 1 (Association Rules)
try:
    from models.model_1_rules.infer import AssociationRecommender
    association_model = AssociationRecommender()
    print("✅ Model 1 (Association Rules) loaded successfully!")
except Exception as e:
    association_model = None
    print(f"⚠️ Model 1 không khả dụng: {e}")

# Load pre-computed recommendations cache
try:
    with open(CACHE_PATH, 'r', encoding='utf-8') as f:
        USER_CACHE = json.load(f)
    print(f"✅ Cache loaded: {len(USER_CACHE)} users pre-computed")
except Exception as e:
    USER_CACHE = {}
    print(f"⚠️ Cache not found: {e}")

# Import Model 2 (ALS - Collaborative Filtering)
try:
    from models.model_2_als.infer import ALSRecommender
    als_model = ALSRecommender()
    print("✅ Model 2 (ALS) loaded successfully!")
except Exception as e:
    als_model = None
    print(f"⚠️ Model 2 không khả dụng: {e}")

TMDB_API_KEY = "ff48b02cdcd1f6e40df93cb3ff292031"
BASE_URL = "https://api.themoviedb.org/3"
NUMBER_RECOMMENDATIONS = 10  # Số phim gợi ý cho mỗi user

# FastAPI app
app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------- Async TMDB requests ---------------- #

async def tmdb_get_movie(client: httpx.AsyncClient, tmdb_id: int):
    try:
        res = await client.get(
            f"{BASE_URL}/movie/{tmdb_id}",
            params={
                "api_key": TMDB_API_KEY,
                "language": "vi-VN",
                "append_to_response": "images"
            },
            timeout=15.0
        )
        if res.status_code == 200:
            return res.json()
        else:
            print(f"⚠️ TMDB error for ID {tmdb_id}: {res.status_code}")
            return {}
    except Exception as e:
        print(f"⚠️ TMDB exception for ID {tmdb_id}: {e}")
        return {}

async def tmdb_get_trailer_key(client: httpx.AsyncClient, tmdb_id: int):
    res = await client.get(
        f"{BASE_URL}/movie/{tmdb_id}/videos",
        params={
            "api_key": TMDB_API_KEY,
            "language": "en-US"
        },
        timeout=15.0
    )
    data = res.json().get("results", [])

    for v in data:
        if v["site"] == "YouTube" and v["type"] == "Trailer":
            return v["key"]
    for v in data:
        if v["site"] == "YouTube":
            return v["key"]
    return None

async def parse_movie_detail(client: httpx.AsyncClient, tmdb_id: int, movie_id: int = None):
    m = await tmdb_get_movie(client, tmdb_id)
    
    # Skip nếu TMDB không trả về data hợp lệ
    if not m or not m.get("title"):
        return None
    
    trailer_key = await tmdb_get_trailer_key(client, tmdb_id)
    
    # Nếu không truyền movie_id, tìm từ tmdb_id
    if movie_id is None:
        movie_id = tmdb_to_movieId.get(tmdb_id)

    return {
        "id": movie_id,  # Thêm trường id
        "movieId": movie_id,  # Thêm trường movieId cho backward compatibility
        "title": m.get("title"),
        "original_title": m.get("original_title"),
        "release_date": m.get("release_date"),
        "vote_average": m.get("vote_average"),
        "vote_count": m.get("vote_count"),
        "genres": [g["name"] for g in m.get("genres", [])],
        "trailer_key": trailer_key,
        "overview": m.get("overview"),
        "poster": f"https://image.tmdb.org/t/p/w500{m['poster_path']}" if m.get("poster_path") else None,
        "backdrop": f"https://image.tmdb.org/t/p/w1280{m['backdrop_path']}" if m.get("backdrop_path") else None,
        "logo": (
            f"https://image.tmdb.org/t/p/w300{m['images']['logos'][0]['file_path']}"
            if m.get("images", {}).get("logos") else None
        )
    }

# ---------------- Recommendation ---------------- #

def recommend_for_user(user_id, k=10):
    """Gợi ý phim cho user - Ưu tiên cache, fallback sang Model 2 real-time"""
    POPULAR_MOVIES = [1, 356, 296, 318, 593, 260, 480, 527, 150, 110]
    
    # STRATEGY 1: Load từ cache (SIÊU NHANH - 0.001s)
    user_key = str(user_id)
    if user_key in USER_CACHE:
        print(f"⚡ Load cache cho User {user_id} (instant)")
        # Cache chứa movie titles, cần convert sang movieIds
        cached_titles = USER_CACHE[user_key][:k]
        movie_ids = []
        for title in cached_titles:
            if title in title_to_movieId:
                movie_ids.append(title_to_movieId[title])
        print(f"✅ Converted {len(movie_ids)}/{len(cached_titles)} titles to IDs")
        return movie_ids if movie_ids else POPULAR_MOVIES[:k]
    
    # STRATEGY 2: Chạy Model 2 real-time (CHO USER MỚI)
    if als_model and als_model.model:
        try:
            print(f"🔍 User {user_id} chưa có cache, chạy Model 2...")
            results = als_model.recommend_for_user(user_id, top_k=k)
            
            if results:
                recommended_movie_ids = []
                for result in results:
                    movie_title = result['movie']
                    if movie_title in title_to_movieId:
                        recommended_movie_ids.append(title_to_movieId[movie_title])
                
                if len(recommended_movie_ids) >= k:
                    print(f"✅ Model 2 trả về {len(recommended_movie_ids)} phim")
                    return recommended_movie_ids[:k]
        except Exception as e:
            print(f"⚠️ Lỗi Model 2: {e}")
    
    # STRATEGY 3: Fallback phim phổ biến
    print(f"⚠️ Dùng fallback cho User {user_id}")
    return POPULAR_MOVIES[:k]

def recommend_similar_movies(movie_id, k=5):
    """Tìm phim tương tự - Sử dụng Model 1 (Association Rules)"""
    
    # Fallback: Top phim phổ biến khi không có gợi ý từ model
    POPULAR_MOVIES = [1, 356, 296, 318, 593, 260, 480, 527, 150, 110]
    
    # Nếu model chưa train hoặc không load được, dùng fallback
    if association_model is None or association_model.rules is None:
        print(f"⚠️ Model 1 chưa sẵn sàng, dùng top phim phổ biến")
        return POPULAR_MOVIES[:k]
    
    try:
        # Chuyển movieId sang tên phim
        if movie_id not in movieId_to_title:
            print(f"⚠️ Không tìm thấy movie_id {movie_id}, dùng fallback")
            return POPULAR_MOVIES[:k]
        
        movie_title = movieId_to_title[movie_id]
        print(f"🔍 Tìm phim tương tự cho: {movie_title}")
        
        # Gọi model để lấy gợi ý
        results = association_model.recommend(movie_title, top_k=k)
        
        if not results:
            print(f"⚠️ Model không có luật cho phim này, dùng top phim phổ biến")
            # Loại bỏ chính phim hiện tại khỏi danh sách
            fallback = [mid for mid in POPULAR_MOVIES if mid != movie_id]
            return fallback[:k]
        
        # Chuyển tên phim về movieId
        recommended_movie_ids = []
        for result in results:
            rec_title = result['movie']
            if rec_title in title_to_movieId:
                rec_id = title_to_movieId[rec_title]
                if rec_id != movie_id:  # Không gợi ý chính nó
                    recommended_movie_ids.append(rec_id)
        
        print(f"✅ Tìm thấy {len(recommended_movie_ids)} phim từ Association Rules")
        
        # Nếu model trả về ít hơn k phim, bổ sung bằng top phim phổ biến
        if len(recommended_movie_ids) < k:
            for mid in POPULAR_MOVIES:
                if mid not in recommended_movie_ids and mid != movie_id:
                    recommended_movie_ids.append(mid)
                    if len(recommended_movie_ids) >= k:
                        break
        
        return recommended_movie_ids[:k]
        
    except Exception as e:
        print(f"⚠️ Lỗi khi gọi Model 1: {e}")
        # Fallback về top phim phổ biến
        fallback = [mid for mid in POPULAR_MOVIES if mid != movie_id]
        return fallback[:k]

# ---------------- API endpoint ---------------- #

@app.get("/api/search/movies")
async def search_movies(q: str, limit: int = 10):
    """Tìm kiếm phim theo tên"""
    if not q or len(q) < 2:
        return []
    
    query = q.lower()
    results = []
    
    for movie_id, title in movieId_to_title.items():
        if query in title.lower():
            results.append({
                "movieId": movie_id,
                "title": title
            })
            if len(results) >= limit:
                break
    
    return results

@app.get("/api/recommend/user/{user_id}")
async def get_user_recommendations(user_id: int):
    """API gợi ý phim cho user"""
    try:
        # Lấy danh sách movie_id gợi ý
        rec_movie_ids = recommend_for_user(user_id, k=10)
        
        # Chuyển sang tmdb_id và giữ movieId để pass vào parse_movie_detail
        movie_tmdb_pairs = [(mid, movieId_to_tmdb[mid]) for mid in rec_movie_ids if mid in movieId_to_tmdb]
        
        if not movie_tmdb_pairs:
            return []
        
        # Lấy thông tin chi tiết từ TMDB
        async with httpx.AsyncClient() as client:
            tasks = [parse_movie_detail(client, tmdb_id, movie_id) for movie_id, tmdb_id in movie_tmdb_pairs]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Lọc kết quả thành công (bỏ qua None và Exception)
        movies = [r for r in results if r and not isinstance(r, Exception)]
        
        return movies
        
    except Exception as e:
        raise HTTPException(500, f"Error: {str(e)}")

@app.get("/api/movies/popular")
async def get_popular_movies():
    """API lấy top phim phổ biến (không chạy model, instant response)"""
    # Top phim kinh điển phổ biến nhất
    POPULAR_MOVIE_IDS = [
        318,   # The Shawshank Redemption
        858,   # The Godfather
        260,   # Star Wars: Episode IV
        480,   # Jurassic Park
        593,   # The Silence of the Lambs
        527,   # Schindler's List
        296,   # Pulp Fiction
        356,   # Forrest Gump
        1,     # Toy Story
        110,   # Braveheart
        150,   # Apollo 13
        457,   # The Fugitive
    ]
    
    try:
        movie_tmdb_pairs = [(mid, movieId_to_tmdb[mid]) for mid in POPULAR_MOVIE_IDS if mid in movieId_to_tmdb]
        
        async with httpx.AsyncClient() as client:
            tasks = [parse_movie_detail(client, tmdb_id, movie_id) for movie_id, tmdb_id in movie_tmdb_pairs]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        movies = [r for r in results if r and not isinstance(r, Exception)]
        return movies
    except Exception as e:
        raise HTTPException(500, f"Error: {str(e)}")

@app.get("/api/movie/{movieId}")
async def movie_detail(movieId: int):
    """API xem chi tiết phim và phim liên quan"""
    if movieId not in movieId_to_tmdb:
        raise HTTPException(404, "Movie not found")

    tmdb_id = movieId_to_tmdb[movieId]

    # Lấy phim tương tự
    rec_movie_ids = recommend_similar_movies(movieId, k=5)
    rec_movie_tmdb_pairs = [(mid, movieId_to_tmdb[mid]) for mid in rec_movie_ids if mid in movieId_to_tmdb]

    async with httpx.AsyncClient() as client:
        # Song song lấy phim chính + phim liên quan
        tasks = [parse_movie_detail(client, tmdb_id, movieId)]  # phim chính
        tasks += [parse_movie_detail(client, t_id, m_id) for m_id, t_id in rec_movie_tmdb_pairs]  # phim liên quan

        results = await asyncio.gather(*tasks, return_exceptions=True)

    movie_detail_result = results[0]
    related_results = [r for r in results[1:] if not isinstance(r, Exception)]

    # Trả về frontend
    return {
        "movie": movie_detail_result,
        "related": related_results
    }

# Mount thư mục ui để truy cập test.html
app.mount("/static", StaticFiles(directory=BASE_DIR), name="static")

@app.get("/")
def index():
    return FileResponse(os.path.join(BASE_DIR, "test.html"))
