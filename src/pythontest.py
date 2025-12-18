import os
import pandas as pd

# --- CẤU HÌNH ---
RAW_PATH = "../data"
OUTPUT_PATH = "../data"

MIN_USER_RATINGS = 5
MIN_MOVIE_RATINGS = 10
print("📂 Đang đọc dữ liệu CSV...")

ratings = pd.read_csv(os.path.join(RAW_PATH, "ratings.csv"))
movies  = pd.read_csv(os.path.join(RAW_PATH, "movies.csv"))
links   = pd.read_csv(os.path.join(RAW_PATH, "links.csv"))
print("🧹 Đang lọc dữ liệu...")

user_counts = ratings["userId"].value_counts()
movie_counts = ratings["movieId"].value_counts()
valid_users = user_counts[user_counts >= MIN_USER_RATINGS].index
valid_movies = movie_counts[movie_counts >= MIN_MOVIE_RATINGS].index

ratings_clean = ratings[
    ratings["userId"].isin(valid_users) &
    ratings["movieId"].isin(valid_movies)
][["userId", "movieId", "rating", "timestamp"]]
print(f"✅ Dữ liệu sạch còn lại: {len(ratings_clean)} dòng rating.")
print("🛠  Đang tạo file movieId–tmdbId...")
movie_tmdb = (
    ratings_clean[["movieId"]]
    .drop_duplicates()
    .merge(
        links[["movieId", "tmdbId"]],
        on="movieId",
        how="inner"
    )
    .dropna(subset=["tmdbId"])
    .sort_values("movieId")
)
output_path = os.path.join(OUTPUT_PATH, "movie_tmdb_filtered.csv")
movie_tmdb.to_csv(output_path, index=False)

print(f"✅ Đã lưu file tại: {output_path}")
