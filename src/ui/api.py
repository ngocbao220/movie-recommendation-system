from fastapi import FastAPI, HTTPException


import pandas as pd
import requests

import os
import pandas as pd

# Thư mục chứa api.py → src/ui
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Lùi 2 cấp: ui → src → project root
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))

# data/ nằm ở project root
DATA_PATH = os.path.join(PROJECT_ROOT, "data", "movies.csv")

df_map = pd.read_csv(DATA_PATH)
df_map["tmdbId"] = df_map["tmdbId"].astype(int)


movieId_to_tmdb = dict(
    zip(df_map["movieId"], df_map["tmdbId"])
)

TMDB_API_KEY = "ff48b02cdcd1f6e40df93cb3ff292031"
BASE_URL = "https://api.themoviedb.org/3"

def tmdb_get_movie(tmdb_id):
    res = requests.get(
        f"{BASE_URL}/movie/{tmdb_id}",
        params={
            "api_key": TMDB_API_KEY,
            "language": "vi-VN",
            "append_to_response": "images"
        },
        timeout=15
    )
    return res.json()

def tmdb_get_trailer_key(tmdb_id):
    res = requests.get(
        f"{BASE_URL}/movie/{tmdb_id}/videos",
        params={
            "api_key": TMDB_API_KEY,
            "language": "en-US"
        },
        timeout=15
    )

    data = res.json().get("results", [])

    # Ưu tiên Trailer trên YouTube
    for v in data:
        if v["site"] == "YouTube" and v["type"] == "Trailer":
            return v["key"]

    # Fallback: teaser
    for v in data:
        if v["site"] == "YouTube":
            return v["key"]

    return None


def parse_movie_detail(m):
    return {
        "title": m.get("title"),
        "original_title": m.get("original_title"),
        "release_date": m.get("release_date"),
        "vote_average": m.get("vote_average"),
        "vote_count": m.get("vote_count"),
        "genres": [g["name"] for g in m.get("genres", [])],
        "overview": m.get("overview"),
        "poster": f"https://image.tmdb.org/t/p/w500{m['poster_path']}" if m.get("poster_path") else None,
        "backdrop": f"https://image.tmdb.org/t/p/w1280{m['backdrop_path']}" if m.get("backdrop_path") else None,
        "logo": (
            f"https://image.tmdb.org/t/p/w300{m['images']['logos'][0]['file_path']}"
            if m.get("images", {}).get("logos") else None
        )
    }

def recommend(movie_id, k=10):
    return [2, 34, 56, 78]


app = FastAPI()

@app.get("/api/movie/{movieId}")
def movie_detail(movieId: int):

    # 1. movieId → tmdbId
    if movieId not in movieId_to_tmdb:
        raise HTTPException(404, "Movie not found")

    tmdb_id = movieId_to_tmdb[movieId]

    # 2. TMDB detail
    movie_raw = tmdb_get_movie(tmdb_id)
    movie_trailer_key = tmdb_get_trailer_key(tmdb_id)
    movie_detail = parse_movie_detail(movie_raw)

    # 3. Model recommend
    rec_movie_ids = recommend(movieId)

    # 4. Lấy info cho phim đề xuất
    recommendations = []
    for mid in rec_movie_ids:
        if mid not in movieId_to_tmdb:
            continue

        tmdb_rec = movieId_to_tmdb[mid]
        m = tmdb_get_movie(tmdb_rec)

        recommendations.append({
            "movieId": mid,
            "title": m.get("title"),
            "poster": f"https://image.tmdb.org/t/p/w300{m['poster_path']}" if m.get("poster_path") else None,
            "vote_average": m.get("vote_average"),
            "vote_count": m.get("vote_count")
        })

    # 5. Trả về frontend
    return {
        "movie": movie_detail,
        "recommendations": recommendations
    }


# tmdb_id = 862
# print(tmdb_get_movie(tmdb_id))
# print(tmdb_get_trailer_key(tmdb_id))