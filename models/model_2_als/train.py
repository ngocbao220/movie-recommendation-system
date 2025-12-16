import os
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

# --- CẤU HÌNH ---
INPUT_PATH = "data/processed/model2_als"
OUTPUT_PATH = "models/model_2_als/artifacts/als_model"

def main():
    print("🚀 Đang khởi động Spark cho Model 2 (ALS)...")
    # Với ALS, chúng ta cần nhiều RAM cho executor
    spark = SparkSession.builder \
        .appName("Train_Model_2_ALS") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .getOrCreate()

    # 1. Load dữ liệu
    print(f"📂 Đang đọc dữ liệu từ {INPUT_PATH}...")
    df = spark.read.parquet(INPUT_PATH)
    
    # Chia tập train/test để kiểm tra độ chính xác
    (training, test) = df.randomSplit([0.8, 0.2], seed=42)
    
    # Cache lại training set vì ALS sẽ lặp qua nó nhiều lần (MaxIter)
    training.cache()
    print(f"✅ Đã load dữ liệu. Training set: {training.count()} dòng.")

    # 2. Cấu hình thuật toán ALS
    # rank: Số lượng đặc trưng ẩn (càng cao càng chính xác nhưng tốn RAM)
    # maxIter: Số vòng lặp học
    # regParam: Tham số chống học vẹt (Overfitting)
    als = ALS(maxIter=10, 
              rank=10,
              regParam=0.1, 
              userCol="userId", 
              itemCol="movieId", 
              ratingCol="rating",
              coldStartStrategy="drop", # Bỏ qua user mới trong tập test để không bị lỗi NaN
              nonnegative=True)         # Rating không âm

    # 3. Train
    print("⏳ Đang train mô hình ALS (Matrix Factorization)...")
    model = als.fit(training)

    # 4. Đánh giá lỗi (RMSE)
    print("📊 Đang đánh giá độ chính xác trên tập Test...")
    predictions = model.transform(test)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    
    print(f"🎉 Kết quả: Root-mean-square error (RMSE) = {rmse:.4f}")
    print("(RMSE càng nhỏ càng tốt. Ví dụ: 0.8 nghĩa là dự đoán lệch trung bình 0.8 sao)")

    # 5. Lưu Model
    print(f"💾 Đang lưu model vào {OUTPUT_PATH}...")
    model.write().overwrite().save(OUTPUT_PATH)
    print("✅ Lưu thành công!")

    spark.stop()

if __name__ == "__main__":
    main()