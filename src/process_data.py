import os
import re
import gc # Import Garbage Collector to force memory cleanup
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer
from pyspark.sql.types import StringType

# --- CONFIGURATION ---
RAW_PATH = "data/raw"
OUTPUT_PATH = "data/processed"

# Filtering Thresholds
MIN_USER_RATINGS = 5
MIN_MOVIE_RATINGS = 10
MIN_RATING_FOR_RULES = 3.5

def clean_title_logic(title):
    if not title: return title
    title = title.strip()
    match = re.match(r'^(.*),\s(The|A|An|Les|Le|La)\s\((\d{4})\)$', title)
    if match: return f"{match.group(2)} {match.group(1)} ({match.group(3)})"
    match_no_year = re.match(r'^(.*),\s(The|A|An|Les|Le|La)$', title)
    if match_no_year: return f"{match_no_year.group(2)} {match_no_year.group(1)}"
    return title

def main():
    print("🚀 Starting Spark Session...")
    # Keep 4g. If you crash again, ensure your Docker/Host has enough RAM.
    spark = SparkSession.builder \
        .appName("MovieLens_Preprocessing") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.driver.maxResultSize", "2g") \
        .getOrCreate()

    # 1. LOAD DATA
    print("📂 Reading CSV data...")
    # Ensure these paths exist in your container
    df_ratings = spark.read.csv(os.path.join(RAW_PATH, "ratings.csv"), header=True, inferSchema=True)
    df_movies = spark.read.csv(os.path.join(RAW_PATH, "movies.csv"), header=True, inferSchema=True)

    clean_title_udf = F.udf(clean_title_logic, StringType())
    df_movies = df_movies.withColumn("title", clean_title_udf(F.col("title")))
    
    # 2. FILTERING
    print(f"🧹 Filtering data...")
    user_counts = df_ratings.groupBy("userId").count().withColumnRenamed("count", "user_count")
    movie_counts = df_ratings.groupBy("movieId").count().withColumnRenamed("count", "movie_count")
    
    df_clean = df_ratings \
        .join(user_counts, "userId", "inner").filter(F.col("user_count") >= MIN_USER_RATINGS) \
        .join(movie_counts, "movieId", "inner").filter(F.col("movie_count") >= MIN_MOVIE_RATINGS) \
        .select("userId", "movieId", "rating", "timestamp")

    # CACHE HERE for Model 1 & 2
    df_clean.cache()
    print(f"✅ Cleaned data count: {df_clean.count()} rows.")

    # --- MODEL 1: ASSOCIATION RULES ---
    print("\n🛠  Model 1 (Rules)...")
    df_rules = df_clean.filter(F.col("rating") >= MIN_RATING_FOR_RULES).join(df_movies, "movieId", "inner")
    
    df_transactions = df_rules.groupBy("userId").agg(F.collect_list("title").alias("items"))
    df_transactions.write.mode("overwrite").parquet(os.path.join(OUTPUT_PATH, "model1_rules"))
    print(f"✅ Model 1 Saved.")

    # --- MODEL 2: ALS ---
    print("\n🛠  Model 2 (ALS)...")
    # Save this data to disk. We will read it back for Model 3.
    model2_path = os.path.join(OUTPUT_PATH, "model2_als")
    df_clean.select("userId", "movieId", "rating").write.mode("overwrite").parquet(model2_path)
    print(f"✅ Model 2 Saved.")

    # =========================================================
    # CRITICAL FIX: FREE MEMORY BEFORE MODEL 3
    # =========================================================
    print("\n🧹 Unpersisting cache to free RAM for StringIndexer...")
    df_clean.unpersist()        # Release the dataframe from memory
    spark.catalog.clearCache()  # Clear all other caches
    gc.collect()                # Force Python to clean up garbage
    # =========================================================

    # --- MODEL 3: DEEP LEARNING (INDEXING) ---
    print("\n🛠  Model 3 (Deep Learning / StringIndexer)...")
    
    # Read back from the Parquet file we just saved (Much more memory efficient than cached CSV)
    print("   -> Reloading data from Parquet (Disk)...")
    df_for_ncf = spark.read.parquet(model2_path)

    print("   -> Indexing UserId...")
    # setHandleInvalid("skip") prevents crashes on unseen/null labels
    user_indexer = StringIndexer(inputCol="userId", outputCol="userIndex").setHandleInvalid("skip")
    user_indexer_model = user_indexer.fit(df_for_ncf)
    df_indexed = user_indexer_model.transform(df_for_ncf)
    
    print("   -> Indexing MovieId...")
    movie_indexer = StringIndexer(inputCol="movieId", outputCol="movieIndex").setHandleInvalid("skip")
    movie_indexer_model = movie_indexer.fit(df_indexed)
    df_final_ncf = movie_indexer_model.transform(df_indexed)
    
    # Save Indexed Data
    df_final_ncf.select(
        F.col("userIndex").cast("integer"), 
        F.col("movieIndex").cast("integer"), 
        F.col("rating")
    ).write.mode("overwrite").parquet(os.path.join(OUTPUT_PATH, "model3_ncf"))
    
    # Save Mappings (FIXED LOGIC to ensure Index ID matches Row ID)
    print("   -> Saving Mappings...")
    
    # Create tuples of (index, label) explicitly using enumerate
    user_labels = [(idx, label) for idx, label in enumerate(user_indexer_model.labels)]
    spark.createDataFrame(user_labels, ["userIndex", "original_userId"]) \
        .write.mode("overwrite").parquet(os.path.join(OUTPUT_PATH, "model3_ncf_user_mapping"))
        
    movie_labels = [(idx, label) for idx, label in enumerate(movie_indexer_model.labels)]
    spark.createDataFrame(movie_labels, ["movieIndex", "original_movieId"]) \
        .write.mode("overwrite").parquet(os.path.join(OUTPUT_PATH, "model3_ncf_movie_mapping"))

    print(f"✅ Model 3 Saved.")
    spark.stop()
    print("\n🎉 PROCESS COMPLETED SUCCESSFULLY!")

if __name__ == "__main__":
    main()