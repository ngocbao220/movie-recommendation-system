import os 

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
MODEL_DIR = os.path.join(BASE_DIR, 'models')

#paths
MOVIES_PATH = os.path.join(DATA_DIR, 'movies.csv')
RATINGS_PATH = os.path.join(DATA_DIR, 'ratings.csv')
TAGS_PATH = os.path.join(DATA_DIR, 'tags.csv')

ASSOCIATION_RULES_PARQUET = os.path.join(MODEL_DIR, 'association_rules_v3.parquet')
METADATA_PATH = os.path.join(MODEL_DIR, 'metadata.parquet')

# --- TỐI ƯU HÓA THAM SỐ (QUAN TRỌNG) ---

# 1. Giảm số lượng user để chạy nhanh hơn
# KHUYẾN NGHỊ:
# - 30k-50k users: An toàn, chạy trong 10-20 phút
# - 100k users: Rủi ro cao, có thể treo
# - 200k+ users: Rất dễ treo hoặc hết RAM
TOP_USERS = 50000 # GIẢM từ 50k để tránh treo

# 2. Support - Càng cao càng nhanh nhưng ít rules
# KHUYẾN NGHỊ:
# - 0.05-0.10: Rất nhanh (< 5 phút) nhưng chỉ bắt phim hot
# - 0.03: Cân bằng tốt (10-15 phút)
# - 0.02: Chậm, có thể treo nếu TOP_USERS lớn
MIN_SUPPORT = 0.06  # TĂNG từ 0.02 để an toàn hơn

# 3. Confidence giữ nguyên hoặc giảm nhẹ
MIN_CONFIDENCE = 0.3


# =================== CẤU HÌNH CHO ĐƯỜNG DẪN ===================
ALS_RESULT = os.path.join(BASE_DIR, "data/results/als_recommendations")
RULES_RESULT = os.path.join(BASE_DIR, "data/results/rules_recommendations")
LINKS_PATH = os.path.join(BASE_DIR, "data/raw/links.csv")

API_KEY: str = "ff48b02cdcd1f6e40df93cb3ff292031"
BASE_URL: str = "https://api.themoviedb.org/3"
IMAGE_BASE_W300: str = "https://image.tmdb.org/t/p/w300"
IMAGE_BASE_W500: str = "https://image.tmdb.org/t/p/w500"
IMAGE_BASE_W1280: str = "https://image.tmdb.org/t/p/w1280"

# =================== CẤU HÌNH CHO MODEL 2: ALS ===================
INPUT_PATH_2 = "data/processed/model2_als"
RESULT_PATH_2 = "data/results/als_recommendations" # Nơi lưu kết quả cuối cùng cho App
MODEL_SAVE_PATH_2 = "checkpoints/model_2_als"

NUMBER_RECOMMENDATIONS = 10  # Số gợi ý cho mỗi user