import pandas as pd
import os
import json
import logging

logger = logging.getLogger(__name__)

class RecommenderService:
    def __init__(self, als_path, rules_path, links_path):
        """
        als_path: Đường dẫn đến kết quả ALS
        rules_path: Đường dẫn đến kết quả Association Rules
        links_path: Đường dẫn đến file links.csv (chứa movieId, imdbId, tmdbId)
        """
        logger.info("📂 RecommenderService initializing...")
        logger.info(f"  ALS path: {als_path}")
        logger.info(f"  Rules path: {rules_path}")
        logger.info(f"  Links path: {links_path}")
        
        # Cache for popular movies and user recommendations
        self._popular_cache = None
        self._popular_cache_key = None
        self._user_recs_cache = {}
        
        # Load cache files if available
        self._load_cache_files()
        
        # 1. Load Mapping ID từ links.csv thay vì movies.csv
        if os.path.exists(links_path):
            try:
                df_links = pd.read_csv(links_path)
                logger.info(f"📊 Loaded {len(df_links)} rows from links.csv")
                # Xử lý: bỏ dòng thiếu tmdbId, ép kiểu về int để gọi API TMDB chính xác
                df_links = df_links.dropna(subset=['tmdbId'])
                df_links['tmdbId'] = df_links['tmdbId'].astype(int)
                self.movieId_to_tmdb = dict(zip(df_links["movieId"], df_links["tmdbId"]))
                logger.info(f"✅ Loaded mapping for {len(self.movieId_to_tmdb)} movies")
            except Exception as e:
                logger.error(f"❌ Error loading links.csv: {str(e)}")
                self.movieId_to_tmdb = {}
        else:
            self.movieId_to_tmdb = {}
            logger.error(f"❌ Links file not found at {links_path}")

        # 2. Load kết quả ALS (Cá nhân hóa)
        if os.path.exists(als_path):
            try:
                self.als_df = pd.read_parquet(als_path).set_index("userId")
                logger.info(f"✅ Loaded ALS data: {len(self.als_df)} users")
            except Exception as e:
                logger.error(f"❌ Error loading ALS data: {str(e)}")
                self.als_df = None
        else:
            logger.warning(f"⚠️ ALS path not found: {als_path}")
            self.als_df = None

        # 3. Load kết quả Association Rules (Phim tương đương)
        if os.path.exists(rules_path):
            try:
                self.rules_df = pd.read_parquet(rules_path).set_index("movieId")
                logger.info(f"✅ Loaded Rules data: {len(self.rules_df)} movies")
            except Exception as e:
                logger.error(f"❌ Error loading Rules data: {str(e)}")
                self.rules_df = None
        else:
            logger.warning(f"⚠️ Rules path not found: {rules_path}")
            self.rules_df = None
        
        # Load precomputed cache files
        self._load_cache_files()

    def _load_cache_files(self):
        """Load precomputed cache from JSON files"""
        cache_dir = os.path.join(os.getcwd(), "data", "cache")
        
        # Load popular movies cache (overwrite memory cache with file cache)
        popular_cache_path = os.path.join(cache_dir, "popular_movies.json")
        if os.path.exists(popular_cache_path):
            try:
                with open(popular_cache_path, 'r', encoding='utf-8') as f:
                    popular_data = json.load(f)
                # Store as precomputed cache
                self._popular_cache = popular_data
                self._popular_cache_key = "precomputed"
                logger.info(f"💾 Loaded popular movies cache: {len(popular_data)} movies")
            except Exception as e:
                logger.warning(f"⚠️ Could not load popular cache: {e}")
        
        # Load user recommendations cache
        self._user_recs_cache = {}
        user_cache_path = os.path.join(cache_dir, "user_recommendations.json")
        if os.path.exists(user_cache_path):
            try:
                with open(user_cache_path, 'r', encoding='utf-8') as f:
                    self._user_recs_cache = json.load(f)
                logger.info(f"💾 Loaded user recommendations cache: {len(self._user_recs_cache)} users")
            except Exception as e:
                logger.warning(f"⚠️ Could not load user cache: {e}")

    def get_tmdb_id(self, movie_id: int):
        return self.movieId_to_tmdb.get(movie_id)

    def get_user_recommendations(self, user_id: int):
        # Check cache first for faster response (users 1-10)
        if str(user_id) in self._user_recs_cache:
            logger.info(f"💾 Using cached recommendations for user {user_id}")
            return self._user_recs_cache[str(user_id)]
        
        # Fall back to ALS model for other users
        if self.als_df is not None and user_id in self.als_df.index:
            # Chuyển về list thuần túy (convert numpy types to Python int)
            return [int(x) for x in self.als_df.loc[user_id, "recommendations"]]
        return []

    def get_related_movies(self, movie_id: int):
        if self.rules_df is not None and movie_id in self.rules_df.index:
            # Giả sử cột kết quả trong parquet của bạn tên là 'items' (như script trước)
            # Convert numpy types to Python int
            return [int(x) for x in self.rules_df.loc[movie_id, "items"]]
        return []
    
    def search_movies(self, query: str, movies_path: str = "data/movies.csv", limit: int = 10):
        """Search movies by title"""
        if not os.path.exists(movies_path):
            return []
        
        df = pd.read_csv(movies_path)
        # Case-insensitive search
        mask = df['title'].str.contains(query, case=False, na=False)
        results = df[mask].head(limit)
        
        return [
            {
                "movieId": int(row["movieId"]),
                "title": row["title"],
                "tmdbId": self.get_tmdb_id(int(row["movieId"]))
            }
            for _, row in results.iterrows()
        ]

    def get_popular_movies(self, top_n: int = 20, min_ratings: int = 5, ratings_path: str = None):
        """
        Trả về danh sách phim phổ biến nhất theo số lượng đánh giá.

        - top_n: số lượng kết quả trả về
        - min_ratings: lọc những phim có ít nhất `min_ratings` lượt đánh giá
        - ratings_path: đường dẫn tới file ratings.csv (mặc định tìm `data/raw/ratings.csv` dưới working dir)

        Trả về list các dict với các keys: movieId, tmdbId (nếu có), count, avg_rating
        """
        if ratings_path is None:
            ratings_path = os.path.join(os.getcwd(), "data", "raw", "ratings.csv")

        # Check if we have precomputed cache and can use it
        if self._popular_cache_key == "precomputed" and self._popular_cache is not None:
            # If request is for same or fewer movies than cache, slice it
            if min_ratings == 5 and top_n <= len(self._popular_cache):
                logger.info(f"💾 Using precomputed cache (sliced to {top_n} from {len(self._popular_cache)} movies)")
                return self._popular_cache[:top_n]
            # Exact match
            elif top_n == 20 and min_ratings == 5:
                logger.info(f"💾 Using precomputed popular movies cache: {len(self._popular_cache)} movies")
                return self._popular_cache
        
        # Check runtime cache (key includes top_n and min_ratings)
        cache_key = (ratings_path, top_n, min_ratings)
        if self._popular_cache_key == cache_key and isinstance(self._popular_cache, list):
            logger.info(f"💾 Using runtime cached popular movies (top_n={top_n})")
            return self._popular_cache

        if not os.path.exists(ratings_path):
            return []

        df = pd.read_csv(ratings_path)
        grp = (
            df.groupby("movieId").rating
            .agg(["count", "mean"]).reset_index()
            .rename(columns={"count": "count", "mean": "avg_rating"})
        )
        grp = grp[grp["count"] >= min_ratings]
        grp = grp.sort_values(["count", "avg_rating"], ascending=[False, False])
        top = grp.head(top_n)

        results = []
        for _, row in top.iterrows():
            movie_id = int(row["movieId"])
            results.append({
                "movieId": movie_id,
                "tmdbId": self.get_tmdb_id(movie_id),
                "count": int(row["count"]),
                "avg_rating": float(row["avg_rating"]),
            })

        # Cache the result
        self._popular_cache = results
        self._popular_cache_key = cache_key
        logger.info(f"💾 Cached {len(results)} popular movies")

        return results