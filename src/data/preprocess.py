import os
import re
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

# --- CẤU HÌNH ---
RAW_PATH = "data/raw"
OUTPUT_PATH = "data/processed"

# Các thư mục con tương ứng với từng Model
MODEL_PATHS = {
    "m1": os.path.join(OUTPUT_PATH, "model1_rules"),
    "m2": os.path.join(OUTPUT_PATH, "model2_als"),
}

# Ngưỡng lọc dữ liệu
MIN_USER_RATINGS = 5
MIN_MOVIE_RATINGS = 10
MIN_RATING_FOR_RULES = 3.5

def clean_title_logic(title):
    if not title: return title
    title = title.strip()
    match = re.match(r'^(.*),\s(The|A|An|Les|Le|La)\s\((\d{4})\)$', title)
    if match: return f"{match.group(2)} {match.group(1)} ({match.group(3)})"
    match_no_year = re.match(r'^(.*),\s(The|A|An|Les|Le|La)$', title)
    if match_no_year: return f"{match_no_year.group(2)} {match_no_year.group(1)}"
    return title

def check_processed_data():
    """Kiểm tra xem dữ liệu sạch cho cả 3 model đã tồn tại chưa"""
    if not os.path.exists(OUTPUT_PATH):
        return False
    
    for name, path in MODEL_PATHS.items():
        # Kiểm tra thư mục có tồn tại và có chứa file (không rỗng) không
        if not os.path.exists(path) or not os.listdir(path):
            print(f"⚠️  Thiếu dữ liệu tại: {path}")
            return False
    return True

def main():
    # 1. KIỂM TRA DỮ LIỆU SẠCH TRƯỚC
    data_exists = check_processed_data()
    
    if data_exists:
        print("✅ Dữ liệu sạch (Processed Data) đã tồn tại đầy đủ.")
        return
    else:
        print("Status: 🆕 Dữ liệu sạch chưa có hoặc chưa đủ. Bắt đầu xử lý...")

    # 2. KIỂM TRA DỮ LIỆU THÔ (RAW) TRƯỚC KHI BẬT SPARK
    required_raw = ["ratings.csv", "movies.csv"]
    for f in required_raw:
        if not os.path.exists(os.path.join(RAW_PATH, f)):
            print(f"❌ Lỗi: Không tìm thấy {f} trong {RAW_PATH}. Vui lòng kiểm tra lại!")
            sys.exit(1)

    # 3. KHỞI TẠO SPARK SESSION
    print("\n🚀 Đang khởi động Spark Session (Cấp phát 8GB RAM)...")
    spark = SparkSession.builder \
        .appName("MovieLens_Preprocessing") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

    try:
        # --- ĐỌC DỮ LIỆU ---
        print("📂 Đang đọc dữ liệu thô...")
        df_ratings = spark.read.csv(os.path.join(RAW_PATH, "ratings.csv"), header=True, inferSchema=True)
        df_movies = spark.read.csv(os.path.join(RAW_PATH, "movies.csv"), header=True, inferSchema=True)
        clean_title_udf = F.udf(clean_title_logic, StringType())
        df_movies = df_movies.withColumn("title", clean_title_udf(F.col("title")))

        # --- LỌC DỮ LIỆU CHUNG ---
        print(f"🧹 Đang lọc (User >= {MIN_USER_RATINGS} rates, Movie >= {MIN_MOVIE_RATINGS} rates)...")
        user_counts = df_ratings.groupBy("userId").count().withColumnRenamed("count", "user_count")
        movie_counts = df_ratings.groupBy("movieId").count().withColumnRenamed("count", "movie_count")
        
        df_clean = df_ratings \
            .join(user_counts, "userId", "inner").filter(F.col("user_count") >= MIN_USER_RATINGS) \
            .join(movie_counts, "movieId", "inner").filter(F.col("movie_count") >= MIN_MOVIE_RATINGS) \
            .select("userId", "movieId", "rating", "timestamp")

        df_clean.cache()
        print(f"✅ Dữ liệu sau lọc: {df_clean.count()} dòng.")

        # --- NHÁNH 1: ASSOCIATION RULES ---
        print("\n🛠  Model 1 (Rules) - Hybrid Selection (Best + Recent)...")
    
        # Bước 1: Chỉ lấy phim User đã thích (Rating >= 3.5)
        # Loại bỏ phim dở để tránh học luật sai
        df_high_rating = df_clean.filter(F.col("rating") >= 3.5)
        
        # Join lấy tên phim
        df_rules_joined = df_high_rating.join(df_movies, "movieId", "inner") \
                                        .select("userId", "title", "rating", "timestamp")
        
        # Cửa sổ 1: Xếp theo Rating giảm dần (Ưu tiên phim 5 sao)
        # Nếu rating bằng nhau, phim nào mới hơn xếp trước
        w_best = Window.partitionBy("userId").orderBy(F.col("rating").desc(), F.col("timestamp").desc())
        
        # Cửa sổ 2: Xếp theo Thời gian giảm dần (Ưu tiên phim mới xem)
        w_recent = Window.partitionBy("userId").orderBy(F.col("timestamp").desc())
        
        df_ranked = df_rules_joined \
            .withColumn("rank_best", F.row_number().over(w_best)) \
            .withColumn("rank_recent", F.row_number().over(w_recent))
        
        # LẤY HỢP (UNION) CỦA 2 NHÓM:
        # - Nhóm 1: Top 30 phim hay nhất (Giữ huyền thoại)
        # - Nhóm 2: Top 20 phim mới nhất (Giữ xu hướng)
        # Tổng cộng tối đa 50 phim/user (Nếu phim vừa hay vừa mới thì càng tốt)
        df_smart_selected = df_ranked.filter(
            (F.col("rank_best") <= 50) 
        )
        
        # Bước 3: Gom nhóm
        # Dùng collect_set để tự động khử trùng lặp (nếu phim nằm trong cả 2 top)
        df_transactions = df_smart_selected.groupBy("userId") \
                                        .agg(F.collect_set("title").alias("items"))
        
        # Lưu ra disk
        path_m1 = os.path.join(OUTPUT_PATH, "model1_rules")
        df_transactions.write.mode("overwrite").parquet(path_m1)
        
        print(f"✅ Model 1 Saved: Đã chọn lọc tinh hoa .")
        # --- NHÁNH 2: SPARK ALS ---
        print("🛠  Xử lý Model 2 (Spark ALS)...")
        df_clean.select("userId", "movieId", "rating").write.mode("overwrite").parquet(MODEL_PATHS["m2"])

        # --- NHÁNH 3: NEURAL CF (DEEP LEARNING) ---
        # print("🛠  Xử lý Model 3 (Neural CF)...")
        # # Indexing liên tục cho User và Movie
        # u_indexer = StringIndexer(inputCol="userId", outputCol="userIndex").fit(df_clean)
        # m_indexer = StringIndexer(inputCol="movieId", outputCol="movieIndex").fit(df_clean)
        
        # df_ncf = u_indexer.transform(df_clean)
        # df_ncf = m_indexer.transform(df_ncf)
        
        # # Lưu dữ liệu chính
        # df_ncf.select(
        #     F.col("userIndex").cast("integer"), 
        #     F.col("movieIndex").cast("integer"), 
        #     F.col("rating")
        # ).write.mode("overwrite").parquet(MODEL_PATHS["m3"])
        
        # # Lưu Mapping để tra cứu sau này
        # spark.createDataFrame([(i, ) for i in u_indexer.labels], ["original_userId"]) \
        #     .write.mode("overwrite").parquet(MODEL_PATHS["m3_u_map"])
        # spark.createDataFrame([(i, ) for i in m_indexer.labels], ["original_movieId"]) \
        #     .write.mode("overwrite").parquet(MODEL_PATHS["m3_m_map"])

        print(f"\n🎉 HOÀN TẤT! Dữ liệu sạch đã được lưu tại: {OUTPUT_PATH}")

    except Exception as e:
        print(f"❌ Lỗi trong quá trình xử lý: {str(e)}")
    finally:
        spark.stop()
        print("🔌 Spark Session đã đóng.")

if __name__ == "__main__":
    main()