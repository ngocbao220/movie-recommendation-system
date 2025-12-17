import sys
import os
import time
from pyspark.sql import SparkSession
from pyspark.ml.fpm import FPGrowth
from pyspark.sql import functions as F

# --- CẤU HÌNH ---
INPUT_PATH = "data/processed/model1_rules"
OUTPUT_PATH = "checkpoints/model_1_rules/rules.parquet"

# --- THIẾT LẬP HỢP LÝ ---
# Khi đã cắt ngắn dữ liệu, ta có thể hạ Support xuống mức hợp lý hơn
MIN_SUPPORT = 0.03      # 3% (Thay vì 0.1)
MIN_CONFIDENCE = 0.3    # 30%

# GIỚI HẠN DỮ LIỆU ĐỂ CHỐNG OOM
MAX_ITEMS_PER_USER = 50   # Chỉ lấy 50 phim/user
USER_SAMPLE_FRACTION = 0.5 # Chỉ lấy 50% số lượng user

def main():
    print("🚀 Đang khởi động Spark (Optimized Mode)...")
    spark = SparkSession.builder \
        .appName("Train_Model_1_Rules") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "100") \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "1g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # 1. Đọc dữ liệu
    print(f"📂 Đang đọc dữ liệu từ {INPUT_PATH}...")
    if not os.path.exists(INPUT_PATH):
        print("❌ Lỗi: Không tìm thấy data.")
        return

    df = spark.read.parquet(INPUT_PATH)
    total_users = df.count()
    print(f"📊 Tổng user ban đầu: {total_users}")
    
    # 2. XỬ LÝ DỮ LIỆU (CHÌA KHÓA CHỐNG CRASH)
    print("✂️ Đang tối ưu hóa dữ liệu...")
    
    # Bước A: Lấy mẫu ngẫu nhiên (Sampling)
    df_sampled = df.sample(withReplacement=False, fraction=USER_SAMPLE_FRACTION, seed=42)
    
    # Bước B: Cắt ngắn Transaction (Slicing) & Xóa trùng lặp
    # slice(col, start, length): Lấy từ phần tử số 1, lấy tối đa 50 phần tử
    df_clean = df_sampled.withColumn("items_distinct", F.array_distinct(F.col("items"))) \
                         .withColumn("items_sliced", F.slice(F.col("items_distinct"), 1, MAX_ITEMS_PER_USER)) \
                         .select(F.col("items_sliced").alias("items"))
    
    # Cache lại dữ liệu sạch
    df_clean.cache()
    clean_count = df_clean.count()
    print(f"✅ Dữ liệu sau khi xử lý: {clean_count} users (Max {MAX_ITEMS_PER_USER} items/user).")

    # 3. TRAIN
    print(f"🛠  Bắt đầu Train FPGrowth (Support: {MIN_SUPPORT}, Conf: {MIN_CONFIDENCE})...")
    start_time = time.time()
    
    fp = FPGrowth(itemsCol="items", 
                  minSupport=MIN_SUPPORT, 
                  minConfidence=MIN_CONFIDENCE)
    
    model = fp.fit(df_clean)
    print(f"⏱  Train xong trong {round(time.time() - start_time, 2)} giây.")

    # 4. LƯU KẾT QUẢ
    print("💾 Đang sinh luật và lưu...")
    try:
        rules = model.associationRules
        
        # --- TỐI ƯU BỘ NHỚ KHI LƯU ---
        # Chỉ giữ lại các luật ngắn (Antecedent <= 2)
        # Luật dài (VD: Xem A,B,C,D -> E) rất tốn bộ nhớ và ít có giá trị gợi ý thực tế
        print("   -> Lọc bỏ các luật quá dài (Antecedent > 2)...")
        rules_filtered = rules.filter(F.size(F.col("antecedent")) <= 2)
        
        # Lưu ra Parquet
        rules_filtered.write.mode("overwrite").parquet(OUTPUT_PATH)
        print("✅ Lưu thành công!")
        
        # 5. KIỂM TRA
        print("📊 Kiểm tra kết quả...")
        saved_rules = spark.read.parquet(OUTPUT_PATH)
        count_rules = saved_rules.count()
        print(f"🎉 Tổng số luật tìm thấy: {count_rules}")
        
        if count_rules > 0:
            saved_rules.sort(F.col("lift").desc()).show(5, truncate=False)
            
    except Exception as e:
        print(f"❌ Lỗi khi lưu: {e}")
        import traceback
        traceback.print_exc()

    spark.stop()

if __name__ == "__main__":
    main()