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

# --- CẤU HÌNH "GROWTH MODE" (CÂN BẰNG GIỮA SỐ LƯỢNG VÀ CHẤT LƯỢNG) ---

# 1. Dùng 100% dữ liệu
USER_SAMPLE_FRACTION = 1.0 

# 2. Support 1.5%: Đủ thấp để bắt được phim Marvel, Harry Potter
MIN_SUPPORT = 0.015

# 3. Confidence 30%: Đảm bảo độ tin cậy khá (Xem A thì 40% sẽ xem B)
MIN_CONFIDENCE = 0.3   

# 4. Lift 1.5: Lọc bỏ các cặp phim "xã giao", chỉ giữ lại quan hệ thân thiết
MIN_LIFT = 1.5           

def main():
    # 0. Dọn dẹp thư mục tạm để tránh lỗi ổ cứng
    if os.path.exists(TEMP_DIR): shutil.rmtree(TEMP_DIR)
    os.makedirs(TEMP_DIR)

    print(f"🚀 Khởi động Spark (Growth Mode - Full Data)...")
    
    spark = SparkSession.builder \
        .appName("Train_Rules_Final") \
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

    # 1. ĐỌC DỮ LIỆU
    print(f"📂 Đang đọc dữ liệu từ {INPUT_PATH}...")
    if not os.path.exists(INPUT_PATH):
        print("❌ Lỗi: Không tìm thấy file dữ liệu đầu vào.")
        return

    df = spark.read.parquet(INPUT_PATH)
    
    # 2. XỬ LÝ DỮ LIỆU
    print(f"✂️ Đang chuẩn bị dữ liệu...")
    
    # Lấy mẫu (Nếu cần test nhanh, giảm fraction xuống. Chạy thật thì để 1.0)
    
    
    # CHÚ Ý: Vì process_data.py đã lọc Top 50 phim hay nhất/mới nhất rồi
    # nên ta KHÔNG dùng F.slice ở đây nữa. Chỉ cần array_distinct để an toàn.
    df_clean = df.withColumn("items", F.array_distinct(F.col("items")))
    
    # Repartition & Checkpoint để tối ưu bộ nhớ
    df_clean = df_clean.repartition(200).checkpoint()
    
    count = df_clean.count()
    print(f"✅ Sẵn sàng train trên: {count} users.")

    # 3. TRAIN FPGROWTH
    print(f"🛠  Bắt đầu Train FPGrowth (Supp={MIN_SUPPORT}, Conf={MIN_CONFIDENCE})...")
    start_time = time.time()
    
    fp = FPGrowth(itemsCol="items", 
                  minSupport=MIN_SUPPORT, 
                  minConfidence=MIN_CONFIDENCE)
    
    try:
        model = fp.fit(df_clean)
        print(f"⏱  Train xong trong {round(time.time() - start_time, 2)} giây.")

        # 4. LỌC VÀ LƯU KẾT QUẢ
        print("💾 Đang sinh luật và lọc...")
        rules = model.associationRules
        
        # --- BỘ LỌC CHẤT LƯỢNG ---
        # 1. Antecedent <= 2: Giữ luật ngắn gọn, dễ hiểu
        rules = rules.filter(F.size(F.col("antecedent")) <= 2)
        
        # 2. Lift >= 2.0: Chỉ lấy mối quan hệ mạnh
        rules = rules.filter(F.col("lift") >= MIN_LIFT)
        
        # (Đã bỏ bộ lọc support thừa vì minSupport đã chặn dưới rồi)

        # Lưu kết quả
        # Repartition(5) giúp gom thành 5 file lớn, đọc nhanh hơn là 200 file nhỏ
        rules = rules.repartition(5)
        rules.write.mode("overwrite").parquet(OUTPUT_PATH)
        
        print(f"✅ LƯU THÀNH CÔNG TẠI: {OUTPUT_PATH}")
        
        # 5. KIỂM TRA NHANH
        saved = spark.read.parquet(OUTPUT_PATH)
        print(f"🎉 Tổng số luật tìm được: {saved.count()}")
        
        # Dọn dẹp rác
        if os.path.exists(TEMP_DIR): shutil.rmtree(TEMP_DIR)

    except Exception as e:
        print(f"❌ LỖI QUÁ TRÌNH TRAIN: {e}")

    spark.stop()

if __name__ == "__main__":
    main()