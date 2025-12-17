import os
import sys
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.ml.evaluation import RegressionEvaluator

# --- CẤU HÌNH ---
INPUT_PATH = "data/processed/model2_als"
OUTPUT_PATH = "outputs/model_2_als"

def check_model_exists():
    """Kiểm tra xem thư mục model đã tồn tại và có nội dung chưa"""
    if os.path.exists(OUTPUT_PATH):
        # Kiểm tra xem folder có chứa metadata/data (đặc trưng của Spark model) không
        if os.path.exists(os.path.join(OUTPUT_PATH, "metadata")):
            return True
    return False

def main():
    # 1. KIỂM TRA MODEL TRƯỚC
    if check_model_exists():
        print(f"✅ Model ALS đã tồn tại tại '{OUTPUT_PATH}'.")
        
        # Nếu chạy trong Docker hoặc môi trường tự động
        if not sys.stdin.isatty():
            print("🤖 Docker detected: Bỏ qua bước training.")
            return
            
        # Nếu chạy thủ công bên ngoài
        retrain = input("❓ Bạn có muốn train lại không? (y/n): ").lower()
        if retrain != 'y':
            print("🚀 Sử dụng model cũ. Kết thúc.")
            return

    # 2. KHỞI TẠO SPARK (Chỉ khởi tạo khi thực sự cần train)
    print("🚀 Đang khởi động Spark cho Model 2 (ALS)...")
    spark = SparkSession.builder \
        .appName("Train_Model_2_ALS") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .getOrCreate()

    try:
        # 3. Load dữ liệu
        if not os.path.exists(INPUT_PATH):
            print(f"❌ Lỗi: Không thấy dữ liệu đầu vào tại {INPUT_PATH}")
            return

        print(f"📂 Đang đọc dữ liệu từ {INPUT_PATH}...")
        df = spark.read.parquet(INPUT_PATH)
        
        # Chia tập train/test
        (training, test) = df.randomSplit([0.8, 0.2], seed=42)
        training.cache()
        print(f"✅ Đã load dữ liệu. Training set: {training.count()} dòng.")

        # 4. Cấu hình thuật toán ALS
        als = ALS(maxIter=10, 
                  rank=10,
                  regParam=0.1, 
                  userCol="userId", 
                  itemCol="movieId", 
                  ratingCol="rating",
                  coldStartStrategy="drop",
                  nonnegative=True)

        # 5. Train
        print("⏳ Đang train mô hình ALS (Matrix Factorization)...")
        model = als.fit(training)

        # 6. Đánh giá lỗi (RMSE)
        print("📊 Đang đánh giá độ chính xác trên tập Test...")
        predictions = model.transform(test)
        evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
        rmse = evaluator.evaluate(predictions)
        
        print(f"🎉 Kết quả: Root-mean-square error (RMSE) = {rmse:.4f}")

        # 7. Lưu Model
        print(f"💾 Đang lưu model vào {OUTPUT_PATH}...")
        model.write().overwrite().save(OUTPUT_PATH)
        print("✅ Lưu thành công!")

    except Exception as e:
        print(f"❌ Lỗi khi training: {e}")
    finally:
        spark.stop()
        print("🔌 Spark Session đã đóng.")

if __name__ == "__main__":
    main()