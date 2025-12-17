from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

import pandas as pd
import os
import httpx
import asyncio

# Thư mục chứa api.py → src/ui
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Lùi 2 cấp: ui → src → project root
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))

# data/ nằm ở project root
DATA_PATH = os.path.join(PROJECT_ROOT, "data", "movies.csv")

df_map = pd.read_csv(DATA_PATH)
df_map["tmdbId"] = df_map["tmdbId"].astype(int)
movieId_to_tmdb = dict(zip(df_map["movieId"], df_map["tmdbId"]))

TMDB_API_KEY = "ff48b02cdcd1f6e40df93cb3ff292031"
BASE_URL = "https://api.themoviedb.org/3"

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

# Mount static
app.mount("/static", StaticFiles(directory=BASE_DIR), name="static")

@app.get("/")
def index():
    return FileResponse(os.path.join(BASE_DIR, "test.html"))

# ---------------- Async TMDB requests ---------------- #

async def tmdb_get_movie(client: httpx.AsyncClient, tmdb_id: int):
    res = await client.get(
        f"{BASE_URL}/movie/{tmdb_id}",
        params={
            "api_key": TMDB_API_KEY,
            "language": "vi-VN",
            "append_to_response": "images"
        },
        timeout=15.0
    )
    return res.json()

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

async def parse_movie_detail(client: httpx.AsyncClient, tmdb_id: int):
    m = await tmdb_get_movie(client, tmdb_id)
    trailer_key = await tmdb_get_trailer_key(client, tmdb_id)

    return {
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

def recommend(movie_id, k=10):
    # Demo tạm
    return [2, 34, 56, 78]

# ---------------- API endpoint ---------------- #

@app.get("/api/movie/{movieId}")
async def movie_detail(movieId: int):
    if movieId not in movieId_to_tmdb:
        raise HTTPException(404, "Movie not found")

    tmdb_id = movieId_to_tmdb[movieId]

    rec_movie_ids = recommend(movieId)
    rec_tmdb_ids = [movieId_to_tmdb[mid] for mid in rec_movie_ids if mid in movieId_to_tmdb]

    async with httpx.AsyncClient() as client:
        # Song song lấy phim chính + trailer + phim đề xuất
        tasks = [parse_movie_detail(client, tmdb_id)]  # phim chính
        tasks += [parse_movie_detail(client, r_id) for r_id in rec_tmdb_ids]  # phim đề xuất

        results = await asyncio.gather(*tasks)

    movie_detail_result = results[0]
    recommendations_result = results[1:]

    # Trả về frontend
    return {
        "movie": movie_detail_result,
        "recommendations": [
            {
                "movieId": mid,
                "title": r["title"],
                "poster": r["poster"],
                "vote_average": r["vote_average"],
                "vote_count": r["vote_count"]
            }
            for mid, r in zip(rec_movie_ids, recommendations_result)
        ]
    }


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Mount thư mục ui để truy cập test.html
app.mount("/static", StaticFiles(directory=BASE_DIR), name="static")

@app.get("/")
def index():
    return FileResponse(os.path.join(BASE_DIR, "test.html"))

# tmdb_id = 862
# print(tmdb_get_movie(tmdb_id))
# print(tmdb_get_trailer_key(tmdb_id))