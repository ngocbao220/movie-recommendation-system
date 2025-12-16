import sys
import os
import time
from pyspark.sql import SparkSession
from pyspark.ml.fpm import FPGrowth
from pyspark.sql import functions as F

# --- CẤU HÌNH ---
INPUT_PATH = "data/processed/model1_rules"
OUTPUT_PATH = "models/model_1_rules/artifacts/rules.parquet"

# TĂNG LÊN 0.05 ĐỂ CHẠY NHANH HƠN (Test luồng)
# Sau khi chạy thành công, bạn có thể giảm xuống 0.02 sau
MIN_SUPPORT = 0.05 
MIN_CONFIDENCE = 0.1

def main():
    print("🚀 Đang khởi động Spark cho Training Model 1...")
    spark = SparkSession.builder \
        .appName("Train_Model_1_Rules") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "100") \
        .getOrCreate()

    print(f"📂 Đang đọc dữ liệu từ {INPUT_PATH}...")
    if not os.path.exists(INPUT_PATH):
        print("❌ Lỗi: Không tìm thấy data.")
        return

    df = spark.read.parquet(INPUT_PATH)
    
    # --- BƯỚC QUAN TRỌNG: CLEAN TRƯỚC, CACHE SAU ---
    print("🧹 Đang loại bỏ phim trùng lặp...")
    df_clean = df.withColumn("items", F.array_distinct(F.col("items")))

    print("💾 Đang nạp dữ liệu vào RAM (Caching)...")
    # Cache dữ liệu sạch để FPGrowth dùng đi dùng lại
    df_clean.cache()
    
    # Gọi count() để ÉP Spark thực thi việc cache ngay lập tức
    count = df_clean.count()
    print(f"✅ Đã cache xong {count} dòng dữ liệu vào RAM.")

    # --- TRAIN ---
    print(f"🛠  Bắt đầu Train FPGrowth (Support: {MIN_SUPPORT})...")
    start_time = time.time()
    
    fp = FPGrowth(itemsCol="items", 
                  minSupport=MIN_SUPPORT, 
                  minConfidence=MIN_CONFIDENCE)

    model = fp.fit(df_clean)
    
    print(f"⏱  Train xong trong {round(time.time() - start_time, 2)} giây.")

    # --- KẾT QUẢ ---
    rules = model.associationRules
    rule_count = rules.count()
    print(f"🎉 Đã tìm thấy {rule_count} luật kết hợp!")

    if rule_count > 0:
        print("--- Top 5 luật mạnh nhất ---")
        rules.sort(F.col("lift").desc()).show(5, truncate=False)
        
        print(f"💾 Đang lưu luật vào {OUTPUT_PATH}...")
        rules.write.mode("overwrite").parquet(OUTPUT_PATH)
        print("✅ Lưu thành công!")
    else:
        print("⚠️ Không tìm thấy luật nào. Hãy giảm minSupport.")

    spark.stop()

if __name__ == "__main__":
    main()