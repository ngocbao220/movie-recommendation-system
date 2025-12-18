import os 

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
MODEL_DIR = os.path.join(BASE_DIR, 'models')

#paths
MOVIES_PATH = os.path.join(DATA_DIR, 'movies.csv')
RATINGS_PATH = os.path.join(DATA_DIR, 'ratings.csv')
TAGS_PATH = os.path.join(DATA_DIR, 'tags.csv')

# =================== CẤU HÌNH CHO ĐƯỜNG DẪN ===================
ALS_RESULT = os.path.join(BASE_DIR, "data/results/als_recommendations")
RULES_RESULT = os.path.join(BASE_DIR, "data/results/rules_recommendations")
LINKS_PATH = os.path.join(BASE_DIR, "data/raw/links.csv")

API_KEY: str = "ff48b02cdcd1f6e40df93cb3ff292031"
BASE_URL: str = "https://api.themoviedb.org/3"
IMAGE_BASE_W300: str = "https://image.tmdb.org/t/p/w300"
IMAGE_BASE_W500: str = "https://image.tmdb.org/t/p/w500"
IMAGE_BASE_W1280: str = "https://image.tmdb.org/t/p/w1280"

# =================== CẤU HÌNH CHO MODEL ===================
# --- CẤU HÌNH CHO MODEL 1: Rules ---
INPUT_PATH_1 = "data/processed/model1_rules"
RESULT_PATH_1 = "checkpoints/model_1_rules" 
MIN_SUPPORT = 0.15  
MIN_CONFIDENCE = 0.1

# --- CẤU HÌNH CHO MODEL 2: ALS ---
INPUT_PATH_2 = "data/processed/model2_als"
RESULT_PATH_2 = "data/results/als_recommendations" # Nơi lưu kết quả cuối cùng cho App
MODEL_SAVE_PATH_2 = "checkpoints/model_2_als"

NUMBER_RECOMMENDATIONS = 20  # Số gợi ý cho mỗi user