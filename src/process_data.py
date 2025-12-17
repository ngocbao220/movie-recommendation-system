import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer

# --- CẤU HÌNH ---
RAW_PATH = "..data"
OUTPUT_PATH = "data/processed"

# Ngưỡng lọc dữ liệu (Để giảm nhiễu và tăng tốc độ train)
MIN_USER_RATINGS = 5   # User phải rate ít nhất 5 phim mới được giữ lại
MIN_MOVIE_RATINGS = 10 # Phim phải có ít nhất 10 người rate mới được giữ lại
MIN_RATING_FOR_RULES = 3.5 # Rating >= 3.5 được coi là "Thích" cho luật kết hợp

def main():
    # 1. Khởi tạo Spark với cấu hình bộ nhớ cao (cho data 32M)
    print("🚀 Đang khởi động Spark Session...")
    spark = SparkSession.builder \
        .appName("MovieLens_Preprocessing") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .getOrCreate()

    # 2. Load dữ liệu thô
    print("📂 Đang đọc dữ liệu từ CSV...")
    df_ratings = spark.read.csv(os.path.join(RAW_PATH, "ratings.csv"), header=True, inferSchema=True)
    df_movies = spark.read.csv(os.path.join(RAW_PATH, "movies.csv"), header=True, inferSchema=True)
    df_links = spark.read.csv(os.path.join(RAW_PATH, "links.csv"),header=True,inferSchema=True)


    # 3. Lọc Dữ liệu chung (Global Filtering)
    # Loại bỏ dữ liệu "rác" (Sparse data) giúp cả 3 model chạy nhanh và chính xác hơn
    print(f"🧹 Đang lọc dữ liệu (Min User Rate: {MIN_USER_RATINGS}, Min Movie Rate: {MIN_MOVIE_RATINGS})...")
    
    # Đếm số rating của user và movie
    user_counts = df_ratings.groupBy("userId").count().withColumnRenamed("count", "user_count")
    movie_counts = df_ratings.groupBy("movieId").count().withColumnRenamed("count", "movie_count")
    
    # Lọc
    df_clean = df_ratings \
        .join(user_counts, "userId", "inner").filter(F.col("user_count") >= MIN_USER_RATINGS) \
        .join(movie_counts, "movieId", "inner").filter(F.col("movie_count") >= MIN_MOVIE_RATINGS) \
        .select("userId", "movieId", "rating", "timestamp") # Chỉ giữ lại cột cần thiết

    # Cache lại vào RAM vì biến này sẽ dùng cho cả 3 nhánh
    df_clean.cache()
    print(f"✅ Dữ liệu sạch còn lại: {df_clean.count()} dòng rating.")

    # ==========================================
    # NHÁNH 1: XỬ LÝ CHO MODEL ASSOCIATION RULES
    # ==========================================
    print("\n🛠  Đang xử lý dữ liệu cho Model 1 (Association Rules)...")
    # Logic: Chỉ lấy phim User thích -> Gom thành list tên phim
    
    # Lấy rating cao và join với tên phim
    df_rules = df_clean.filter(F.col("rating") >= MIN_RATING_FOR_RULES) \
        .join(df_movies, "movieId", "inner")
    
    # Gom nhóm: User | [Phim A, Phim B, Phim C]
    df_transactions = df_rules.groupBy("userId") \
        .agg(F.collect_list("title").alias("items"))
    
    # Lưu
    path_m1 = os.path.join(OUTPUT_PATH, "model1_rules")
    df_transactions.write.mode("overwrite").parquet(path_m1)
    print(f"✅ Đã lưu dữ liệu Model 1 tại: {path_m1}")

    # ==========================================
    # NHÁNH 2: XỬ LÝ CHO MODEL SPARK ALS
    # ==========================================
    print("\n🛠  Đang xử lý dữ liệu cho Model 2 (Spark ALS)...")
    # Logic: Giữ nguyên dạng số (userId, movieId, rating). Spark ALS tự xử lý được ID rời rạc.
    
    path_m2 = os.path.join(OUTPUT_PATH, "model2_als")
    df_clean.select("userId", "movieId", "rating").write.mode("overwrite").parquet(path_m2)
    print(f"✅ Đã lưu dữ liệu Model 2 tại: {path_m2}")

    # ==========================================
    # NHÁNH 3: XỬ LÝ CHO MODEL NEURAL CF
    # ==========================================
    print("\n🛠  Đang xử lý dữ liệu cho Model 3 (Deep Learning)...")
    # Logic: Deep Learning cần index liên tục từ 0 -> N. 
    # userId gốc: 1, 50, 100 -> userId mới: 0, 1, 2
    
    # Indexing User
    user_indexer = StringIndexer(inputCol="userId", outputCol="userIndex")
    user_indexer_model = user_indexer.fit(df_clean)
    df_indexed = user_indexer_model.transform(df_clean)
    
    # Indexing Movie
    movie_indexer = StringIndexer(inputCol="movieId", outputCol="movieIndex")
    movie_indexer_model = movie_indexer.fit(df_indexed)
    df_final_ncf = movie_indexer_model.transform(df_indexed)
    
    # Lưu data đã index (Cast về integer cho nhẹ)
    df_final_ncf = df_final_ncf.select(
        F.col("userIndex").cast("integer"), 
        F.col("movieIndex").cast("integer"), 
        F.col("rating")
    )
    
    path_m3 = os.path.join(OUTPUT_PATH, "model3_ncf")
    df_final_ncf.write.mode("overwrite").parquet(path_m3)
    
    # QUAN TRỌNG: Lưu lại map để sau này tra ngược (Index 0 là phim gì?)
    # Lưu danh sách user gốc (index tương ứng là vị trí trong mảng)
    print("   -> Đang lưu mapping User/Item...")
    spark.createDataFrame([(i, ) for i in user_indexer_model.labels], ["original_userId"]) \
        .write.mode("overwrite").parquet(os.path.join(OUTPUT_PATH, "model3_ncf_user_mapping"))
        
    spark.createDataFrame([(i, ) for i in movie_indexer_model.labels], ["original_movieId"]) \
        .write.mode("overwrite").parquet(os.path.join(OUTPUT_PATH, "model3_ncf_movie_mapping"))

    print(f"✅ Đã lưu dữ liệu Model 3 tại: {path_m3}")

    spark.stop()
    print("\n🎉 XỬ LÝ HOÀN TẤT! Sẵn sàng để train.")

if __name__ == "__main__":
    main()