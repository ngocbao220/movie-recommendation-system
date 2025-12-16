import os
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel
from pyspark.sql.functions import col

# --- CẤU HÌNH ---
MODEL_PATH = "models/model_2_als/artifacts/als_model"
MOVIES_CSV = "data/raw/movies.csv"
RATINGS_CSV = "data/raw/ratings.csv"

class ALSRecommender:
    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        self.model = None
        self.df_movies = None
        self.df_movie_counts = None # Thêm cái này để đếm view
        self.load_model()

    def load_model(self):
        if os.path.exists(MODEL_PATH):
            try:
                print("⏳ Đang load Model và Dữ liệu phụ trợ...")
                self.model = ALSModel.load(MODEL_PATH)
                
                # Load Movies
                self.df_movies = self.spark.read.csv(MOVIES_CSV, header=True, inferSchema=True)
                
                # --- BƯỚC MỚI: TÍNH ĐỘ PHỔ BIẾN ---
                # Load Ratings để đếm xem mỗi phim có bao nhiêu người vote
                df_ratings = self.spark.read.csv(RATINGS_CSV, header=True, inferSchema=True)
                
                # Đếm số lượng rating cho mỗi phim
                self.df_movie_counts = df_ratings.groupBy("movieId").count()
                
                # Join sẵn tên phim và số lượng vote lại với nhau để tra cứu cho nhanh
                # Chỉ giữ lại phim nào có trên 100 lượt vote (Con số này tùy bạn chỉnh)
                self.df_movies = self.df_movies.join(self.df_movie_counts, "movieId") \
                                               .filter(col("count") >= 100) 
                
                self.df_movies.cache()
                print("✅ Model 2 (ALS): Đã load xong. Đã lọc bỏ các phim dưới 100 vote.")
            except Exception as e:
                print(f"❌ Lỗi: {e}")
        else:
            print("⚠️ Chưa có Model.")

    def recommend_for_user(self, user_id, top_k=5):
        if self.model is None: return []

        user_df = self.spark.createDataFrame([(user_id,)], ["userId"])
        
        # Lấy nhiều hơn top_k (ví dụ 100 phim) để sau đó lọc lại những phim phổ biến
        recs_df = self.model.recommendForUserSubset(user_df, 100)
        
        rec_list = recs_df.select("recommendations").collect()
        if not rec_list or not rec_list[0].recommendations: return []
            
        final_results = []
        # Lấy danh sách ID phim được gợi ý
        raw_recs = rec_list[0].recommendations
        
        for row in raw_recs:
            movie_id = row.movieId
            predicted_rating = row.rating
            
            # Tra cứu trong danh sách phim ĐÃ LỌC (trên 100 view)
            movie_info = self.df_movies.filter(col("movieId") == movie_id).first()
            
            # Nếu tìm thấy (tức là phim này đủ phổ biến)
            if movie_info:
                final_results.append({
                    "movie": movie_info['title'],
                    "genres": movie_info['genres'],
                    "score": round(predicted_rating, 2),
                    "votes": movie_info['count'] # Hiển thị thêm số vote cho uy tín
                })
            
            # Nếu đã gom đủ số lượng cần thiết thì dừng
            if len(final_results) >= top_k:
                break
            
        return final_results

# Test thử
if __name__ == "__main__":
    rec = ALSRecommender()
    print("\n🔮 --- KẾT QUẢ GỢI Ý (Đã lọc phim hiếm) ---")
    results = rec.recommend_for_user(1, top_k=10)
    for item in results:
        print(f"🎬 {item['movie']}")
        print(f"   Score: {item['score']} | Votes: {item['votes']} | Genres: {item['genres']}")
        print("-" * 30)