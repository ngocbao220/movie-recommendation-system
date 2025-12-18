import sys
import os
import time
import argparse
from pyspark.sql import SparkSession
from pyspark.ml.fpm import FPGrowth
from pyspark.sql import functions as F


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from setting.config import INPUT_PATH_1, RESULT_PATH_1, MIN_SUPPORT, MIN_CONFIDENCE


def check_results_exist():
    """Kiểm tra xem thư mục kết quả đã có dữ liệu chưa"""
    if os.path.exists(RESULT_PATH_1):
        # Kiểm tra xem có file parquet bên trong không (thư mục không rỗng)
        if os.listdir(RESULT_PATH_1):
            return True
    return False

def main():
    # 0. Xử lý tham số dòng lệnh
    parser = argparse.ArgumentParser(description="Train Association Rules Model")
    parser.add_argument("--train", action="store_true", help="Bắt buộc train lại dù đã có dữ liệu")
    args = parser.parse_args()

    # 1. Kiểm tra dữ liệu cũ
    if check_results_exist() and not args.train:
        print(f"✅ Luật kết hợp đã tồn tại tại: {RESULT_PATH_1}")
        print("🚀 Bỏ qua training. (Sử dụng --train nếu bạn muốn cập nhật luật mới)")
        return

    print("🆕 Bắt đầu quy trình huấn luyện Model 1 (Association Rules)...")

    # 2. Khởi tạo Spark
    spark = SparkSession.builder \
        .appName("Train_Model_1_Rules") \
        .config("spark.driver.memory", "12g") \
        .config("spark.executor.memory", "12g") \
        .config("spark.driver.maxResultSize", "4g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # 3. Đọc dữ liệu
        print(f"📂 Đang đọc dữ liệu từ {INPUT_PATH_1}...")
        if not os.path.exists(INPUT_PATH_1):
            print("❌ Lỗi: Không tìm thấy file dữ liệu đầu vào.")
            return

        df = spark.read.parquet(INPUT_PATH_1)
        
        # Làm sạch dữ liệu: Đảm bảo các mảng phim không bị trùng lặp trong 1 giao dịch
        df_clean = df.withColumn("items", F.array_distinct(F.col("items")))
        df_clean.persist() 

        # 4. Huấn luyện FPGrowth
        print(f"🛠  Bắt đầu Train FPGrowth (Support: {MIN_SUPPORT}, Confidence: {MIN_CONFIDENCE})...")
        start_time = time.time()
        
        fp = FPGrowth(itemsCol="items", 
                      minSupport=MIN_SUPPORT, 
                      minConfidence=MIN_CONFIDENCE)

        model = fp.fit(df_clean)
        print(f"⏱  Train xong trong {round(time.time() - start_time, 2)} giây.")

        # 5. Trích xuất Association Rules
        rules = model.associationRules
        rules.persist()
        
        rule_count = rules.count()
        print(f"🎉 Đã tìm thấy {rule_count} luật kết hợp!")

        if rule_count > 0:
            # 6. Lưu kết quả
            print(f"💾 Đang lưu luật vào {RESULT_PATH_1}...")
            # Coalesce(1) gom cụm lại thành 1 file để API load cho nhanh
            rules.coalesce(1).write.mode("overwrite").parquet(RESULT_PATH_1)
            
            print("--- Top 5 luật có Lift cao nhất ---")
            rules.sort(F.col("lift").desc()).show(5, truncate=False)
            print("✅ Hoàn tất lưu dữ liệu!")
        else:
            print("⚠️ Không tìm thấy luật nào. Gợi ý: Hãy giảm MIN_SUPPORT.")

    except Exception as e:
        print(f"❌ Lỗi: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()