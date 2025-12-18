import httpx
import asyncio
import sys
import os

from fastapi import APIRouter, HTTPException, Depends
from src.services.tmdb import TMDBService
from src.services.recommender import RecommenderService

# Thiết lập PROJECT_ROOT
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from setting.config import ALS_RESULT, RULES_RESULT, LINKS_PATH, API_KEY, BASE_URL, IMAGE_BASE_W300, IMAGE_BASE_W500, IMAGE_BASE_W1280, RATINGS_PATH

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
    tmdb_id = recommender.get_tmdb_id(movie_id)
    if not tmdb_id:
        raise HTTPException(status_code=404, detail="Movie mapping not found")

    related_ids = recommender.get_related_movies(movie_id)
    related_tmdb_ids = [recommender.get_tmdb_id(mid) for mid in related_ids]

    async with httpx.AsyncClient() as client:
        main_task = tmdb_service.get_movie_detail(client, tmdb_id)
        related_tasks = [tmdb_service.get_movie_detail(client, tid) for tid in related_tmdb_ids if tid]
        
        results = await asyncio.gather(main_task, *related_tasks)

    # Tách phim chính và lọc bỏ null ở danh sách liên quan
    main_movie = results[0]
    if not main_movie:
        raise HTTPException(status_code=404, detail="Movie detail not found on TMDB")
        
    related_movies = [m for m in results[1:] if m is not None]

    return {
        "movie": main_movie,
        "related": related_movies
    }

@router.get("/movie/popular/list")
async def get_popular_movies():
    movie_ids = recommender.get_popular_movies(ratings_path=RATINGS_PATH)
    tmdb_ids = [recommender.get_tmdb_id(mid) for mid in movie_ids]

    async with httpx.AsyncClient() as client:
        tasks = [tmdb_service.get_movie_detail(client, tid) for tid in tmdb_ids if tid]
        results = await asyncio.gather(*tasks)

    # Lọc bỏ các giá trị null (phim không tìm thấy)
    return [m for m in results if m is not None]

@router.get("/recommend/user/{user_id}")
async def get_user_recommendations(user_id: int):
    movie_ids = recommender.get_user_recommendations(user_id)
    if not movie_ids:
        movie_ids = recommender.get_popular_movies(ratings_path=RATINGS_PATH, min_ratings=3.0)

    tmdb_ids = [recommender.get_tmdb_id(mid) for mid in movie_ids]

    async with httpx.AsyncClient() as client:
        tasks = [tmdb_service.get_movie_detail(client, tid) for tid in tmdb_ids if tid]
        results = await asyncio.gather(*tasks)

    # Lọc bỏ các giá trị null
    return [m for m in results if m is not None]

@router.get("/recommend/movie/{movie_id}")
async def get_movie_recommendations(movie_id: int):
    movie_ids = recommender.get_related_movies(movie_id)
    if not movie_ids:
        movie_ids = recommender.get_popular_movies(ratings_path=RATINGS_PATH)

    tmdb_ids = [recommender.get_tmdb_id(mid) for mid in movie_ids]

    async with httpx.AsyncClient() as client:
        # Lưu ý: Ở đây tôi đổi thành get_movie_detail để đồng bộ với các hàm trên
        tasks = [tmdb_service.get_movie_detail(client, tid) for tid in tmdb_ids if tid]
        results = await asyncio.gather(*tasks)

    # Lọc bỏ các giá trị null
    return [m for m in results if m is not None]