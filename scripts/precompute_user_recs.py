"""
Pre-compute recommendations cho 10 users đầu tiên (1-10)
Lưu vào JSON để frontend load nhanh
"""
import sys
import os
import json

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from models.model_2_als.infer import ALSRecommender

def precompute_recommendations(user_ids, top_k=10):
    """Pre-compute recommendations cho danh sách users"""
    print(f"🚀 Bắt đầu pre-compute cho {len(user_ids)} users...")
    
    # Load Model 2
    recommender = ALSRecommender()
    
    if recommender.model is None:
        print("❌ Model chưa được train!")
        return
    
    # Lưu kết quả
    results = {}
    
    for user_id in user_ids:
        print(f"\n⏳ Đang tính toán cho User {user_id}...")
        recs = recommender.recommend_for_user(user_id, top_k=top_k)
        
        # Chuyển sang format đơn giản (chỉ lưu movie title)
        movie_titles = [rec['movie'] for rec in recs]
        results[str(user_id)] = movie_titles
        
        print(f"✅ User {user_id}: {len(movie_titles)} phim")
        for i, title in enumerate(movie_titles[:3], 1):
            print(f"   {i}. {title}")
    
    return results

def save_to_json(data, output_path):
    """Lưu vào file JSON"""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    print(f"\n✅ Đã lưu cache vào: {output_path}")

if __name__ == "__main__":
    # Pre-compute cho users 1-10
    user_ids = list(range(1, 11))
    
    # Chạy
    recommendations = precompute_recommendations(user_ids, top_k=10)
    
    if recommendations:
        # Lưu vào data/cache/
        output_path = "data/cache/user_recommendations.json"
        save_to_json(recommendations, output_path)
        
        print("\n" + "="*60)
        print("🎉 HOÀN THÀNH! Cache đã sẵn sàng cho frontend!")
        print("="*60)
