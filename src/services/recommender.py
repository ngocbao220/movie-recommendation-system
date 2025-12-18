import pandas as pd
import os

class RecommenderService:
    def __init__(self, als_path, rules_path, links_path):
        """
        als_path: Đường dẫn đến kết quả ALS
        rules_path: Đường dẫn đến kết quả Association Rules
        links_path: Đường dẫn đến file links.csv (chứa movieId, imdbId, tmdbId)
        """
        print("📂 RecommenderService đang load dữ liệu vào RAM...")
        
        # 1. Load Mapping ID từ links.csv thay vì movies.csv
        if os.path.exists(links_path):
            df_links = pd.read_csv(links_path)
            # Xử lý: bỏ dòng thiếu tmdbId, ép kiểu về int để gọi API TMDB chính xác
            df_links = df_links.dropna(subset=['tmdbId'])
            df_links['tmdbId'] = df_links['tmdbId'].astype(int)
            self.movieId_to_tmdb = dict(zip(df_links["movieId"], df_links["tmdbId"]))
            print(f"✅ Đã load mapping cho {len(self.movieId_to_tmdb)} bộ phim.")
        else:
            self.movieId_to_tmdb = {}
            print(f"❌ Cảnh báo: Không tìm thấy file links tại {links_path}")

        # 2. Load kết quả ALS (Cá nhân hóa)
        if os.path.exists(als_path):
            self.als_df = pd.read_parquet(als_path).set_index("userId")
            print("✅ Đã load dữ liệu ALS.")
        else:
            self.als_df = None

        # 3. Load kết quả Association Rules (Phim tương đương)
        if os.path.exists(rules_path):
            self.rules_df = pd.read_parquet(rules_path).set_index("movieId")
            print("✅ Đã load dữ liệu Rules.")
        else:
            self.rules_df = None

    def get_tmdb_id(self, movie_id: int):
        return self.movieId_to_tmdb.get(movie_id)

    def get_user_recommendations(self, user_id: int):
        if self.als_df is not None and user_id in self.als_df.index:
            # Chuyển về list thuần túy
            return list(self.als_df.loc[user_id, "recommendations"])
        return []

    def get_related_movies(self, movie_id: int):
        if self.rules_df is not None and movie_id in self.rules_df.index:
            # Giả sử cột kết quả trong parquet của bạn tên là 'items' (như script trước)
            return list(self.rules_df.loc[movie_id, "items"])
        return []