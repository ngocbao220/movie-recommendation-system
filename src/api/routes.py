import httpx
import asyncio
import sys
import os
import logging

from fastapi import APIRouter, HTTPException, Depends

logger = logging.getLogger(__name__)

# Thiết lập PROJECT_ROOT
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.services.tmdb import TMDBService
from src.services.recommender import RecommenderService
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
    logger.info(f"📽️ Getting movie detail for movieId: {movie_id}")
    tmdb_id = recommender.get_tmdb_id(movie_id)
    if not tmdb_id:
        logger.warning(f"⚠️ Movie mapping not found for movieId: {movie_id}")
        raise HTTPException(status_code=404, detail="Movie mapping not found")

    # Lấy related movies từ Model 1 (Rules)
    related_ids = recommender.get_related_movies(movie_id)
    
    # Nếu không có rules cho phim này, dùng popular movies làm fallback
    if not related_ids:
        logger.info(f"ℹ️ No rules found for movie {movie_id}, using popular movies as fallback")
        popular_data = recommender.get_popular_movies(ratings_path=RATINGS_PATH, top_n=10)
        related_ids = [item['movieId'] for item in popular_data]
    
    related_tmdb_ids = [(recommender.get_tmdb_id(mid), mid) for mid in related_ids]

    async with httpx.AsyncClient() as client:
        main_task = tmdb_service.get_movie_detail(client, tmdb_id, movie_id)
        related_tasks = [tmdb_service.get_movie_detail(client, tid, mid) for tid, mid in related_tmdb_ids if tid]
        
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
    logger.info("🎬 Getting popular movies...")
    try:
        popular_data = recommender.get_popular_movies(ratings_path=RATINGS_PATH)
        logger.info(f"📊 Found {len(popular_data)} popular movies")
        # Extract movieId from dict results
        tmdb_movie_pairs = [(item['tmdbId'], item['movieId']) for item in popular_data if item.get('tmdbId')]
    except Exception as e:
        logger.error(f"❌ Error getting popular movies: {str(e)}")
        raise

    async with httpx.AsyncClient() as client:
        tasks = [tmdb_service.get_movie_detail(client, tid, mid) for tid, mid in tmdb_movie_pairs if tid]
        results = await asyncio.gather(*tasks)

    # Lọc bỏ các giá trị null (phim không tìm thấy)
    return [m for m in results if m is not None]

@router.get("/search/movies")
async def search_movies(q: str, limit: int = 10):
    """Search movies by title with autocomplete"""
    logger.info(f"🔍 Searching movies: query='{q}', limit={limit}")
    if not q or len(q) < 2:
        logger.warning("⚠️ Query too short")
        return []
    
    results = recommender.search_movies(q, limit=limit)
    logger.info(f"📝 Found {len(results)} movies for query '{q}'")
    return results

@router.get("/recommend/user/{user_id}")
async def get_user_recommendations(user_id: int):
    logger.info(f"👤 Getting recommendations for user: {user_id}")
    try:
        movie_ids = recommender.get_user_recommendations(user_id)
        if not movie_ids:
            logger.warning(f"⚠️ No recommendations found for user {user_id}, using popular movies")
            popular_data = recommender.get_popular_movies(ratings_path=RATINGS_PATH, min_ratings=3.0)
            # Extract movieIds from dict results
            movie_ids = [item['movieId'] for item in popular_data]
        else:
            logger.info(f"✅ Found {len(movie_ids)} recommendations for user {user_id}")
    except Exception as e:
        logger.error(f"❌ Error getting user recommendations: {str(e)}")
        raise

    tmdb_movie_pairs = [(recommender.get_tmdb_id(mid), mid) for mid in movie_ids]

    async with httpx.AsyncClient() as client:
        tasks = [tmdb_service.get_movie_detail(client, tid, mid) for tid, mid in tmdb_movie_pairs if tid]
        results = await asyncio.gather(*tasks)

    # Lọc bỏ các giá trị null
    return [m for m in results if m is not None]

@router.get("/recommend/movie/{movie_id}")
async def get_movie_recommendations(movie_id: int):
    logger.info(f"🎬 Getting recommendations for movie: {movie_id}")
    movie_ids = recommender.get_related_movies(movie_id)
    
    if not movie_ids:
        logger.info(f"ℹ️ No rules found for movie {movie_id}, using popular movies as fallback")
        popular_data = recommender.get_popular_movies(ratings_path=RATINGS_PATH, top_n=20)
        # Extract movieIds from dict results
        movie_ids = [item['movieId'] for item in popular_data]
    else:
        logger.info(f"✅ Found {len(movie_ids)} related movies from rules")

    tmdb_movie_pairs = [(recommender.get_tmdb_id(mid), mid) for mid in movie_ids]

    async with httpx.AsyncClient() as client:
        # Lưu ý: Ở đây tôi đổi thành get_movie_detail để đồng bộ với các hàm trên
        tasks = [tmdb_service.get_movie_detail(client, tid, mid) for tid, mid in tmdb_movie_pairs if tid]
        results = await asyncio.gather(*tasks)

    # Lọc bỏ các giá trị null
    return [m for m in results if m is not None]