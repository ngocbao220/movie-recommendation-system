import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql import functions as F

from config.setting import NUMBER_RECOMMENDATIONS, INPUT_PATH, RESULT_PATH, MODEL_SAVE_PATH

def main():
    # 1. Khởi tạo Spark
    spark = SparkSession.builder \
        .appName("ALS_Final_Recommendations") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .getOrCreate()

    try:
        # 2. Đọc dữ liệu đã xử lý
        print(f"📂 Đang đọc dữ liệu từ {INPUT_PATH}...")
        df = spark.read.parquet(INPUT_PATH)

        # 3. Cấu hình & Train ALS
        # Lưu ý: Ở đây dùng toàn bộ dữ liệu để train (không split) 
        # vì mục tiêu là tạo gợi ý tốt nhất cho dữ liệu tĩnh hiện có.
        print("⏳ Đang train mô hình ALS...")
        als = ALS(
            maxIter=15, 
            rank=20, 
            regParam=0.1, 
            userCol="userId", 
            itemCol="movieId", 
            ratingCol="rating",
            coldStartStrategy="drop",
            nonnegative=True
        )
        model = als.fit(df)

        # 4. TẠO GỢI Ý CHO TẤT CẢ USER (recommendForAllUsers)
        print("🎯 Đang tạo Top 10 gợi ý cho mỗi người dùng...")
        # Hàm này trả về DataFrame: [userId, recommendations]
        # recommendations là một mảng các struct: [movieId, rating]
        userRecs = model.recommendForAllUsers(NUMBER_RECOMMENDATIONS)

        # 5. BIẾN ĐỔI DỮ LIỆU ĐỂ APP DỄ ĐỌC (Flatten)
        # Chuyển từ mảng struct phức tạp sang mảng ID phim đơn giản: [id1, id2, id3...]
        userRecs_simple = userRecs.withColumn(
            "recommendations", 
            F.col("recommendations.movieId")
        )

        # 6. LƯU KẾT QUẢ
        print(f"💾 Đang lưu bảng tra cứu vào {RESULT_PATH}...")
        userRecs_simple.write.mode("overwrite").parquet(RESULT_PATH)
        
        # Opendional: Lưu cả model nếu bạn vẫn muốn dùng sau này
        model.write().overwrite().save(MODEL_SAVE_PATH)

        print("✅ Đã xuất kết quả thành công!")
        
        # Debug thử 5 dòng đầu
        userRecs_simple.show(5, truncate=False)

    except Exception as e:
        print(f"❌ Lỗi: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()