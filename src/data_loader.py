import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

# --- CẤU HÌNH ĐƯỜNG DẪN ---
# Đảm bảo đường dẫn trỏ đúng tới folder chứa file gốc của bạn
RAW_MOVIES_PATH = "data/raw/movies.csv"
RAW_TAGS_PATH = "data/raw/tags.csv"
OUTPUT_PATH = "checkpoints/metadata.parquet"

def main():
    print("🚀 Khởi động Spark để tạo Metadata...")
    spark = SparkSession.builder \
        .appName("Create_Metadata_Parquet") \
        .config("spark.driver.memory", "4g") \
        .master("local[*]") \
        .getOrCreate()

    # 1. ĐỌC DỮ LIỆU
    print("📂 Đang đọc movies.csv và tags.csv...")
    if not os.path.exists(RAW_MOVIES_PATH):
        print(f"❌ Lỗi: Không tìm thấy {RAW_MOVIES_PATH}")
        return

    df_movies = spark.read.csv(RAW_MOVIES_PATH, header=True, inferSchema=True)
    
    # Đọc tags (Nếu không có file tags thì tạo DataFrame rỗng để không bị lỗi code)
    if os.path.exists(RAW_TAGS_PATH):
        df_tags = spark.read.csv(RAW_TAGS_PATH, header=True, inferSchema=True)
    else:
        print("⚠️ Không tìm thấy tags.csv, sẽ chỉ dùng Genres.")
        df_tags = spark.createDataFrame([], schema="userId INT, movieId INT, tag STRING, timestamp LONG")

    # 2. LÀM SẠCH TÊN PHIM (QUAN TRỌNG: PHẢI KHỚP VỚI PROCESS_DATA.PY)
    # Logic: Chuyển "Matrix, The (1999)" -> "The Matrix (1999)"
    # Dùng hàm Spark Native cho nhanh
    print("🧹 Đang chuẩn hóa tên phim...")
    df_movies = df_movies.withColumn(
        "title",
        F.when(
            F.col("title").rlike(r'^(.*),\s(The|A|An|Les|Le|La)\s\((\d{4})\)$'),
            F.regexp_replace(F.col("title"), r'^(.*),\s(The|A|An|Les|Le|La)\s\((\d{4})\)$', '$2 $1 ($3)')
        ).otherwise(
            F.when(
                F.col("title").rlike(r'^(.*),\s(The|A|An|Les|Le|La)$'),
                F.regexp_replace(F.col("title"), r'^(.*),\s(The|A|An|Les|Le|La)$', '$2 $1')
            ).otherwise(F.col("title"))
        )
    )

    # 3. XỬ LÝ GENRES (THỂ LOẠI)
    # Input: "Adventure|Animation|Children|Comedy|Fantasy"
    # Output: ["Adventure", "Animation", "Children", "Comedy", "Fantasy"]
    print("🎨 Đang xử lý Genres...")
    df_movies = df_movies.withColumn(
        "genres_arr", 
        F.split(F.col("genres"), "\|") # Tách chuỗi bằng dấu gạch đứng
    )

    # 4. XỬ LÝ TAGS (THẺ TỪ NGƯỜI DÙNG)
    print("🏷️  Đang xử lý Tags...")
    # - Chuyển về chữ thường
    # - Loại bỏ khoảng trắng thừa
    # - Gom nhóm theo MovieId -> Tạo list các tag không trùng lặp (collect_set)
    df_tags_clean = df_tags.filter(F.col("tag").isNotNull()) \
        .withColumn("tag_clean", F.lower(F.trim(F.col("tag")))) \
        .groupBy("movieId") \
        .agg(F.collect_set("tag_clean").alias("tags_arr"))

    # 5. GỘP DỮ LIỆU (JOIN)
    print("🔗 Đang gộp Movies và Tags...")
    # Join Left: Giữ lại tất cả phim kể cả phim không có tag
    df_final = df_movies.join(df_tags_clean, on="movieId", how="left")

    # 6. TẠO CỘT FEATURES (GENRES + TAGS)
    # Nếu tags_arr là null (do không có tag), thay bằng mảng rỗng để không lỗi khi cộng
    df_final = df_final.withColumn(
        "tags_arr", 
        F.coalesce(F.col("tags_arr"), F.array().cast(ArrayType(StringType())))
    )
    
    # Gộp 2 mảng lại: features = genres_arr + tags_arr
    df_final = df_final.withColumn(
        "features", 
        F.array_distinct(F.concat(F.col("genres_arr"), F.col("tags_arr")))
    )

    # Chỉ lấy cột cần thiết
    df_output = df_final.select("movieId", "title", "features")

    # 7. LƯU FILE PARQUET
    print(f"💾 Đang lưu Metadata tại: {OUTPUT_PATH}")
    # Đảm bảo thư mục cha tồn tại
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    
    # Xóa folder cũ nếu có để tránh lỗi
    if os.path.exists(OUTPUT_PATH):
        # shutil.rmtree(OUTPUT_PATH) # Cẩn thận với lệnh này trên server thật
        pass 

    df_output.write.mode("overwrite").parquet(OUTPUT_PATH)
    
    print("✅ ĐÃ TẠO METADATA THÀNH CÔNG!")
    print("📊 Mẫu dữ liệu:")
    df_output.show(5, truncate=False)
    
    spark.stop()

if __name__ == "__main__":
    main()