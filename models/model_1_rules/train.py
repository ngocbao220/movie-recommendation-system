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
OUTPUT_PATH = "checkpoints/model_1_rulesv4/rules.parquet" # Đổi tên folder v3
TEMP_DIR = os.path.join(os.getcwd(), "spark_temp_data") 

# --- CẤU HÌNH "GROWTH MODE" (PHONG PHÚ HƠN) ---

# 1. Dùng 100% dữ liệu (Quan trọng nhất để luật chính xác)
USER_SAMPLE_FRACTION = 1.0 

# 2. Giảm Support xuống 2% để bắt được nhiều phim hơn
MIN_SUPPORT = 0.015

# 3. Giảm Confidence xuống 20% để luật đa dạng hơn
MIN_CONFIDENCE = 0.4    

# 4. Tăng Lift lên 1.2 chút để lọc bớt luật "nhảm" (trùng ngẫu nhiên)
# Luật phong phú cần đi đôi với chất lượng, Lift > 1.2 là ngưỡng đẹp.
MIN_LIFT = 2.0           

# 5. Giữ nguyên giới hạn 50 phim/user để bảo vệ RAM
MAX_ITEMS_PER_USER = 50   

def main():
    # Dọn dẹp temp cũ
    if os.path.exists(TEMP_DIR): shutil.rmtree(TEMP_DIR)
    os.makedirs(TEMP_DIR)

    print(f"🚀 Khởi động Spark (Growth Mode - Full Data)...")
    
    spark = SparkSession.builder \
        .appName("Train_Rules_Growth") \
        .config("spark.driver.memory", "6g") \
        .config("spark.executor.memory", "6g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.memory.fraction", "0.6") \
        .config("spark.memory.storageFraction", "0.2") \
        .config("spark.local.dir", TEMP_DIR) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir(os.path.join(TEMP_DIR, "checkpoints"))

    print(f"📂 Đang đọc dữ liệu...")
    if not os.path.exists(INPUT_PATH): return

    df = spark.read.parquet(INPUT_PATH)
    
    print(f"✂️ Đang xử lý dữ liệu (Full Data, MaxItems={MAX_ITEMS_PER_USER})...")
    
    # 1. Lấy mẫu (Dùng 100% data nếu fraction = 1.0)
    if USER_SAMPLE_FRACTION < 1.0:
        df = df.sample(withReplacement=False, fraction=USER_SAMPLE_FRACTION, seed=42)
    
    # 2. Slicing & Deduplicate
    df_clean = df.withColumn("items_distinct", F.array_distinct(F.col("items"))) \
                 .withColumn("items_sliced", F.slice(F.col("items_distinct"), 1, MAX_ITEMS_PER_USER)) \
                 .select(F.col("items_sliced").alias("items"))
    
    # 3. Repartition
    df_clean = df_clean.repartition(200)
    
    # 4. Checkpoint (An toàn bộ nhớ)
    df_clean = df_clean.checkpoint()
    
    count = df_clean.count()
    print(f"✅ Sẵn sàng train: {count} users.")

    print(f"🛠  Train FPGrowth (Supp={MIN_SUPPORT}, Conf={MIN_CONFIDENCE})...")
    start_time = time.time()
    
    fp = FPGrowth(itemsCol="items", 
                  minSupport=MIN_SUPPORT, 
                  minConfidence=MIN_CONFIDENCE)
    
    try:
        model = fp.fit(df_clean)
        print(f"⏱  Train xong trong {round(time.time() - start_time, 2)} giây.")

        print("💾 Đang lọc và lưu kết quả...")
        rules = model.associationRules
        
        # Lọc luật ngay trên luồng xử lý
        rules = rules.filter(F.size(F.col("antecedent")) <= 2)
        rules = rules.filter(F.col("lift") >= MIN_LIFT)
        rules = rules.filter(F.col("support") >= 0.005)
        
        # Chia nhỏ file đầu ra để tránh lỗi ghi đĩa
        rules = rules.repartition(5)
        
        rules.write.mode("overwrite").parquet(OUTPUT_PATH)
        print(f"✅ LƯU THÀNH CÔNG TẠI: {OUTPUT_PATH}")
        print(f"   (Tham số: Supp={MIN_SUPPORT}, Conf={MIN_CONFIDENCE}, Full Data)")
        
        # Kiểm tra nhanh
        saved = spark.read.parquet(OUTPUT_PATH)
        print(f"🎉 Số luật tìm được: {saved.count()}")
        
        # Dọn dẹp
        if os.path.exists(TEMP_DIR): shutil.rmtree(TEMP_DIR)

    except Exception as e:
        print(f"❌ LỖI: {e}")

    spark.stop()

if __name__ == "__main__":
    main()