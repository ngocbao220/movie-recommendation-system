import httpx
import asyncio
import sys
import os

from fastapi import APIRouter, HTTPException, Depends
from src.services.tmdb import TMDBService
from src.services.recommender import RecommenderService

# Thiết lập PROJECT_ROOT để import từ config
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from setting.config import ALS_RESULT, RULES_RESULT, LINKS_PATH, API_KEY, BASE_URL, IMAGE_BASE_W300, IMAGE_BASE_W500, IMAGE_BASE_W1280

router = APIRouter(prefix="/api", tags=["Movies & Recommendations"])

tmdb_service = TMDBService(
    api_key=API_KEY,
    base_url=BASE_URL,
    image_base_w300=IMAGE_BASE_W300,
    image_base_w500=IMAGE_BASE_W500,
    image_base_w1280=IMAGE_BASE_W1280
)

recommender = RecommenderService(
    als_path=ALS_RESULT,
    rules_path=RULES_RESULT,
    links_path=LINKS_PATH
)

@router.get("/movie/{movie_id}")
async def get_movie_detail_and_recommendations(movie_id: int):
    """
    API lấy chi tiết 1 phim + các phim liên quan (từ Association Rules)
    """
    # 1. Kiểm tra mapping ID
    tmdb_id = recommender.get_tmdb_id(movie_id)
    if not tmdb_id:
        raise HTTPException(status_code=404, detail="Movie mapping not found")

    # 2. Lấy danh sách phim liên quan (Model 1)
    related_ids = recommender.get_related_movies(movie_id)
    related_tmdb_ids = [recommender.get_tmdb_id(mid) for mid in related_ids]

    # 3. Fetch dữ liệu từ TMDB song song (Async)
    async with httpx.AsyncClient() as client:
        # Task phim chính
        main_task = tmdb_service.get_movie_detail(client, tmdb_id)
        # Tasks phim liên quan
        related_tasks = [tmdb_service.get_movie_detail(client, tid) for tid in related_tmdb_ids if tid]
        
        results = await asyncio.gather(main_task, *related_tasks)

    return {
        "movie": results[0],
        "related": results[1:]
    }

@router.get("/recommend/user/{user_id}")
async def get_user_recommendations(user_id: int):
    """
    API lấy danh sách phim gợi ý cá nhân hóa (từ ALS - Model 2)
    """
    movie_ids = recommender.get_user_recommendations(user_id)
    if not movie_ids:
        # Fallback: Trả về phim phổ biến nếu user mới
        movie_ids = recommender.get_popular_movies()

    tmdb_ids = [recommender.get_tmdb_id(mid) for mid in movie_ids]

    async with httpx.AsyncClient() as client:
        tasks = [tmdb_service.get_movie_detail(client, tid) for tid in tmdb_ids if tid]
        recommendations = await asyncio.gather(*tasks)

    return recommendations

@router.get("/recommend/movie/{movie_id}")
async def get_movie_recommendations(movie_id: int):
    """
    API lấy danh sách phim gợi ý dựa trên một phim cụ thể (từ Rules - Model 1)
    """
    movie_ids = recommender.get_related_movies(movie_id)
    if not movie_ids:
        # Fallback: Trả về phim phổ biến nếu user mới
        movie_ids = recommender.get_popular_movies()

    tmdb_ids = [recommender.get_tmdb_id(mid) for mid in movie_ids]

    async with httpx.AsyncClient() as client:
        tasks = [tmdb_service.parse_movie_detail(client, tid) for tid in tmdb_ids if tid]
        recommendations = await asyncio.gather(*tasks)

    return recommendations