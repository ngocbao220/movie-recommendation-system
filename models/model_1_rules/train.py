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
OUTPUT_PATH = "checkpoints/model_1_rulesv2/rules.parquet"
TEMP_DIR = os.path.join(os.getcwd(), "spark_temp_data") 

# --- CẤU HÌNH AN TOÀN TUYỆT ĐỐI (SURVIVAL MODE) ---
# 1. Tăng Support lên 3% (Mất một số phim ngách, nhưng đảm bảo chạy xong)
MIN_SUPPORT = 0.03      
# 2. Confidence 40% (Giữ luật chất lượng)
MIN_CONFIDENCE = 0.4    
# 3. Lift 1.5 (Chặn phim rác)
MIN_LIFT = 1.5           

# 4. Giảm số lượng phim tính toán xuống 30 (Rất quan trọng để giảm tổ hợp)
MAX_ITEMS_PER_USER = 30   
# 5. Chỉ dùng 50% dữ liệu để train (Nếu chạy thành công mới tăng lên)
USER_SAMPLE_FRACTION = 0.5 

def main():
    # Dọn dẹp temp cũ
    if os.path.exists(TEMP_DIR): shutil.rmtree(TEMP_DIR)
    os.makedirs(TEMP_DIR)

    print(f"🚀 Khởi động Spark (Survival Mode - 8GB)...")
    
    spark = SparkSession.builder \
        .appName("Train_Rules_Survival") \
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
    
    print(f"✂️ Đang xử lý dữ liệu (Sample={USER_SAMPLE_FRACTION}, MaxItems={MAX_ITEMS_PER_USER})...")
    
    # 1. Lấy mẫu
    df = df.sample(withReplacement=False, fraction=USER_SAMPLE_FRACTION, seed=42)
    
    # 2. Slicing & Deduplicate
    df_clean = df.withColumn("items_distinct", F.array_distinct(F.col("items"))) \
                 .withColumn("items_sliced", F.slice(F.col("items_distinct"), 1, MAX_ITEMS_PER_USER)) \
                 .select(F.col("items_sliced").alias("items"))
    
    # 3. Repartition (Chia nhỏ dữ liệu ra 200 gói để Executor không bị nghẹn)
    df_clean = df_clean.repartition(200)
    
    # 4. Checkpoint (Cắt đứt RAM cũ)
    df_clean = df_clean.checkpoint()
    
    count = df_clean.count()
    print(f"✅ Sẵn sàng train: {count} users.")

    print(f"🛠  Train FPGrowth (Supp={MIN_SUPPORT})...")
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
        
        # Chia nhỏ file đầu ra
        rules = rules.repartition(5)
        
        rules.write.mode("overwrite").parquet(OUTPUT_PATH)
        print(f"✅ LƯU THÀNH CÔNG! (Support={MIN_SUPPORT})")
        
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