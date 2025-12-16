import sys
import os
from pyspark.sql import SparkSession
from pyspark.ml.fpm import FPGrowth
from pyspark.sql import functions as F

# --- CẤU HÌNH ---
# Đường dẫn input (đầu ra của bước process_data vừa rồi)
INPUT_PATH = "data/processed/model1_rules"
# Đường dẫn output (nơi lưu file luật kết quả)
OUTPUT_PATH = "models/model_1_rules/artifacts/rules.parquet"

# Tham số mô hình
# minSupport=0.02: Phim/Cặp phim phải xuất hiện trong 2% số lượng giao dịch (khoảng 32M * 0.02 user)
MIN_SUPPORT = 0.02 
# minConfidence=0.1: Nếu xem A, có ít nhất 10% khả năng xem B
MIN_CONFIDENCE = 0.1

def main():
    print("🚀 Đang khởi động Spark cho Training Model 1...")
    spark = SparkSession.builder \
        .appName("Train_Model_1_Rules") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .getOrCreate()

    # 1. Load dữ liệu đã xử lý
    print(f"📂 Đang đọc dữ liệu từ {INPUT_PATH}...")
    if not os.path.exists(INPUT_PATH):
        print(f"❌ Lỗi: Không tìm thấy thư mục {INPUT_PATH}. Hãy chạy process_data.py trước!")
        return

    df = spark.read.parquet(INPUT_PATH)
    # Dữ liệu lúc này có dạng: [userId, items (Array[String])]
    
    print("🧹 Đang loại bỏ các phim trùng lặp trong từng user transaction...")
    # array_distinct: Hàm này sẽ biến ['A', 'B', 'A'] thành ['A', 'B']
    df = df.withColumn("items", F.array_distinct(F.col("items")))
    
    # Cache để chạy nhanh hơn
    df.cache()
    print(f"✅ Đã load {df.count()} giao dịch (user baskets).")

    # 2. Định nghĩa thuật toán FPGrowth
    print(f"🛠  Đang cấu hình FPGrowth (Support: {MIN_SUPPORT}, Confidence: {MIN_CONFIDENCE})...")
    fp = FPGrowth(itemsCol="items", 
                  minSupport=MIN_SUPPORT, 
                  minConfidence=MIN_CONFIDENCE)

    # 3. Train (Giai đoạn tốn thời gian nhất)
    print("⏳ Đang train mô hình (việc này có thể mất vài phút)...")
    model = fp.fit(df)

    # 4. Lấy kết quả luật kết hợp
    # Kết quả gồm các cột: antecedents (nguyên nhân), consequents (kết quả), confidence, lift, support
    rules = model.associationRules
    
    rule_count = rules.count()
    print(f"🎉 Đã tìm thấy {rule_count} luật kết hợp!")

    if rule_count == 0:
        print("⚠️ Cảnh báo: Không tìm thấy luật nào. Hãy thử GIẢM minSupport xuống thấp hơn (vd: 0.01).")
    else:
        # Xem thử 5 luật mạnh nhất (theo Lift)
        print("--- Top 5 luật mạnh nhất ---")
        rules.sort(F.col("lift").desc()).show(5, truncate=False)

        # 5. Lưu kết quả
        print(f"💾 Đang lưu luật vào {OUTPUT_PATH}...")
        # Lưu đè (overwrite) nếu file đã tồn tại
        rules.write.mode("overwrite").parquet(OUTPUT_PATH)
        print("✅ Lưu thành công!")

    spark.stop()

if __name__ == "__main__":
    main()