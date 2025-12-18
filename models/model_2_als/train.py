import os
import sys
import argparse

from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql import functions as F

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from setting.config import NUMBER_RECOMMENDATIONS, INPUT_PATH_2, RESULT_PATH_2, MODEL_SAVE_PATH_2, RESULT_PATH_2

def check_results_exist():
    """Kiểm tra xem file kết quả gợi ý đã tồn tại chưa"""
    if os.path.exists(RESULT_PATH_2):
        # Kiểm tra xem thư mục có file parquet (thường là folder không rỗng)
        if os.listdir(RESULT_PATH_2):
            return True
    return False

def main():
    # 0. Xử lý tham số dòng lệnh
    parser = argparse.ArgumentParser(description="Train ALS Model and Export Recommendations")
    parser.add_argument("--train", action="store_true", help="Bắt buộc train lại dù đã có dữ liệu")
    args = parser.parse_args()

    # 1. Kiểm tra dữ liệu cũ
    results_exist = check_results_exist()
    
    if results_exist and not args.train:
        print(f"✅ Dữ liệu gợi ý đã tồn tại tại: {RESULT_PATH_2}")
        print("🚀 Bỏ qua bước training. (Sử dụng --train nếu muốn train lại)")
        return

    print("🆕 Bắt đầu quy trình huấn luyện mới...")

    # 2. Khởi tạo Spark
    print("🚀 Đang khởi động Spark Session...")
    spark = SparkSession.builder \
        .appName("ALS_Final_Recommendations") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .getOrCreate()

    try:
        # 3. Đọc dữ liệu đã xử lý
        if not os.path.exists(INPUT_PATH_2):
            print(f"❌ Lỗi: Không tìm thấy dữ liệu đầu vào tại {INPUT_PATH_2}")
            return

        print(f"📂 Đang đọc dữ liệu từ {INPUT_PATH_2}...")
        df = spark.read.parquet(INPUT_PATH_2)
        # 4. Cấu hình & Train ALS
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

        # 5. TẠO GỢI Ý CHO TẤT CẢ USER
        print(f"🎯 Đang tạo Top {NUMBER_RECOMMENDATIONS} gợi ý cho mỗi người dùng...")
        userRecs = model.recommendForAllUsers(NUMBER_RECOMMENDATIONS)

        # 6. BIẾN ĐỔI DỮ LIỆU (Flatten)
        userRecs_simple = userRecs.withColumn(
            "recommendations", 
            F.col("recommendations.movieId")
        )

        # 7. LƯU KẾT QUẢ
        print(f"💾 Đang lưu bảng tra cứu vào {RESULT_PATH_2}...")
        userRecs_simple.write.mode("overwrite").parquet(RESULT_PATH_2)
        
        # Lưu model
        print(f"💾 Đang lưu model vào {MODEL_SAVE_PATH_2}...")
        model.write().overwrite().save(MODEL_SAVE_PATH_2)
        print("✅ Đã xuất kết quả thành công!")
        userRecs_simple.show(5, truncate=False)

    except Exception as e:
        print(f"❌ Lỗi trong quá trình training: {e}")
    finally:
        spark.stop()
        print("🔌 Spark Session đã đóng.")

if __name__ == "__main__":
    main()