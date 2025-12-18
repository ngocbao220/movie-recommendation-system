import sys
import os
import time
import shutil
import gc
from pyspark.sql import SparkSession
from pyspark.ml.fpm import FPGrowth
from pyspark.sql import functions as F

# --- CẤU HÌNH ---
INPUT_PATH = "data/processed/model1_rules"
OUTPUT_PATH = "checkpoints/model_1_rules/rules.parquet" 
TEMP_DIR = os.path.join(os.getcwd(), "spark_temp_data") 

# TĂNG minSupport lên mức an toàn để chạy thông luồng trước
# Nếu vẫn lỗi, hãy tăng lên 0.2 hoặc 0.5 để test giao diện trước
MIN_SUPPORT = 0.15  
MIN_CONFIDENCE = 0.1

def main():
    print("🚀 Đang khởi động Spark cho Training Model 1...")
    spark = SparkSession.builder \
        .appName("Train_Model_1_Rules") \
        .config("spark.driver.memory", "12g") \
        .config("spark.executor.memory", "12g") \
        .config("spark.driver.maxResultSize", "4g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "4g") \
        .getOrCreate()

    # Giảm mức Log để dễ theo dõi lỗi thực sự
    spark.sparkContext.setLogLevel("ERROR")

    print(f"📂 Đang đọc dữ liệu từ {INPUT_PATH}...")
    if not os.path.exists(INPUT_PATH):
        print("❌ Lỗi: Không tìm thấy file dữ liệu đầu vào.")
        return

    df = spark.read.parquet(INPUT_PATH)
    
    print("🧹 Đang loại bỏ phim trùng lặp...")
    df_clean = df.withColumn("items", F.array_distinct(F.col("items")))

    print("💾 Đang nạp dữ liệu vào RAM...")
    # Sử dụng MEMORY_AND_DISK để nếu thiếu RAM sẽ ghi tạm ra ổ cứng thay vì sập Java
    df_clean.persist() 
    
    try:
        count = df_clean.count()
        print(f"✅ Đã nạp xong {count} dòng dữ liệu.")

        # --- TRAIN ---
        print(f"🛠  Bắt đầu Train FPGrowth (Support: {MIN_SUPPORT})...")
        start_time = time.time()
        
        fp = FPGrowth(itemsCol="items", 
                      minSupport=MIN_SUPPORT, 
                      minConfidence=MIN_CONFIDENCE)

        model = fp.fit(df_clean)
        print(f"⏱  Train xong trong {round(time.time() - start_time, 2)} giây.")

        # --- KẾT QUẢ ---
        # Lưu ý: AssociationRules là phần nặng nhất gây sập Java
        rules = model.associationRules
        
        print("📊 Đang kiểm tra số lượng luật...")
        # Sử dụng persist cho rules trước khi count
        rules.persist()
        rule_count = rules.count()
        print(f"🎉 Đã tìm thấy {rule_count} luật kết hợp!")

        if rule_count > 0:
            print("--- Top 5 luật mạnh nhất ---")
            rules.sort(F.col("lift").desc()).show(5, truncate=False)
            
            print(f"💾 Đang lưu luật vào {OUTPUT_PATH}...")
            # Coalesce(1) giúp lưu thành 1 file duy nhất nếu dữ liệu không quá lớn
            rules.coalesce(1).write.mode("overwrite").parquet(OUTPUT_PATH)
            print("✅ Lưu thành công!")
        else:
            print("⚠️ Không tìm thấy luật nào. Hãy giảm minSupport.")

    except Exception as e:
        print(f"❌ Đã xảy ra lỗi trong quá trình xử lý: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()