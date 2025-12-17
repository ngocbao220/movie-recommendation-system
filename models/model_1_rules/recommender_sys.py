#
import pandas as pd
import numpy as np
import os
import time

# --- CẤU HÌNH ĐƯỜNG DẪN (QUAN TRỌNG) ---
# 1. Trỏ vào model v3 (bản tốt nhất bạn vừa train)
RULES_PATH = "checkpoints/model_1_rulesv3/rules.parquet" 
# 2. Trỏ vào metadata vừa tạo
META_PATH = "checkpoints/metadata.parquet"     

class HybridRecommender:
    def __init__(self):
        print("⚙️  Đang khởi động Hybrid Recommender...")
        self.rules_dict = {}     # Dictionary để tra cứu luật nhanh O(1)
        self.movie_features = {} # Dictionary để tra cứu nội dung nhanh O(1)
        self.load_data()

    def load_data(self):
        start_time = time.time()
        
        # 1. LOAD LUẬT TỪ SPARK (PARQUET)
        if os.path.exists(RULES_PATH):
            try:
                print(f"   -> Đang đọc luật từ: {RULES_PATH}")
                df_rules = pd.read_parquet(RULES_PATH)
                
                # Chuẩn hóa tên cột (antecedent vs antecedents)
                col_ant = 'antecedent' if 'antecedent' in df_rules.columns else 'antecedents'
                col_cons = 'consequent' if 'consequent' in df_rules.columns else 'consequents'
                
                # Chuyển đổi DataFrame sang Dictionary để tìm kiếm siêu tốc
                count = 0
                for _, row in df_rules.iterrows():
                    # antecedent là list, ví dụ: ['Toy Story']
                    inputs = list(row[col_ant])
                    targets = list(row[col_cons])
                    lift = row['lift']
                    
                    for movie in inputs:
                        if movie not in self.rules_dict:
                            self.rules_dict[movie] = []
                        
                        # Lưu tất cả các phim gợi ý (consequents)
                        for target in targets:
                             self.rules_dict[movie].append({
                                'movie': target,
                                'lift': lift
                            })
                    count += 1
                print(f"✅ Đã index xong {count} dòng luật.")
            except Exception as e:
                print(f"❌ Lỗi load Rules: {e}")
        else:
            print(f"⚠️ CẢNH BÁO: Không tìm thấy file luật tại {RULES_PATH}")
        
        # 2. LOAD METADATA (NỘI DUNG)
        if os.path.exists(META_PATH):
            try:
                print(f"   -> Đang đọc metadata từ: {META_PATH}")
                df_meta = pd.read_parquet(META_PATH)
                # Chuyển cột features (đang là list/array) thành set để tính Jaccard nhanh
                self.movie_features = dict(zip(df_meta['title'], df_meta['features']))
                print(f"✅ Đã load thông tin nội dung của {len(df_meta)} phim.")
            except Exception as e:
                print(f"❌ Lỗi load Metadata: {e}")
        else:
            print(f"⚠️ CẢNH BÁO: Không tìm thấy metadata tại {META_PATH}")

        print(f"⏱  Thời gian khởi động: {round(time.time() - start_time, 2)}s")

    def calculate_jaccard(self, movie_a, movie_b):
        """Tính độ giống nhau về nội dung (Genre + Tag)"""
        if movie_a not in self.movie_features or movie_b not in self.movie_features:
            return 0.0 # Không có thông tin thì coi như không giống
        
        set_a = set(self.movie_features[movie_a])
        set_b = set(self.movie_features[movie_b])
        
        intersection = len(set_a.intersection(set_b))
        union = len(set_a.union(set_b))
        
        return intersection / union if union > 0 else 0.0

    def recommend(self, movie_name, top_k=5):
        print(f"\n🎬 Gợi ý cho phim: '{movie_name}'")
        start_time = time.time()
        
        # --- BƯỚC 1: TÌM ỨNG VIÊN TỪ LUẬT (Behavior) ---
        candidates = []
        if movie_name in self.rules_dict:
            candidates = self.rules_dict[movie_name]
        
        # --- BƯỚC 2: FALLBACK (Nếu không có luật) ---
        if not candidates:
            print("⚠️ Không tìm thấy luật kết hợp (Cold Start). Chuyển sang tìm theo Nội dung...")
            return self.recommend_content_only(movie_name, top_k)

        # --- BƯỚC 3: HYBRID RANKING (Kết hợp Luật + Nội dung) ---
        final_results = []
        # Dùng set để tránh trùng lặp khi tính toán
        processed_movies = set()
        
        for item in candidates:
            target_movie = item['movie']
            if target_movie == movie_name or target_movie in processed_movies:
                continue
            
            lift_score = item['lift']
            
            # Tính độ giống nhau về nội dung
            content_sim = self.calculate_jaccard(movie_name, target_movie)
            
            # CÔNG THỨC HYBRID:
            # Score = Lift * (1 + Similarity * 2). 
            # Ví dụ: Nếu giống nhau hoàn toàn (Sim=1), điểm sẽ nhân 3.
            # Mục đích: Đẩy các phim cùng thể loại lên trên, dìm phim khác thể loại xuống.
            hybrid_score = lift_score * (1 + content_sim * 2.0)
            
            final_results.append({
                "Movie": target_movie,
                "Score": round(hybrid_score, 2),
                "Reason": f"Luật (Lift={round(lift_score,1)}) + Nội dung ({round(content_sim*100)}%)"
            })
            processed_movies.add(target_movie)

        # Sắp xếp giảm dần theo điểm Hybrid
        final_results.sort(key=lambda x: x['Score'], reverse=True)
        
        print(f"⏱  Xử lý trong: {(time.time() - start_time)*1000:.2f}ms")
        return final_results[:top_k]

    def recommend_content_only(self, movie_name, top_k):
        """Chỉ dùng nội dung khi phim chưa có luật nào (vd: Phim mới, Iron Man)"""
        if movie_name not in self.movie_features:
            print(f"❌ Phim '{movie_name}' không tồn tại trong dữ liệu.")
            return []
            
        candidates = []
        for other_movie in self.movie_features:
            if other_movie == movie_name: continue
            
            sim = self.calculate_jaccard(movie_name, other_movie)
            
            # Chỉ lấy phim giống trên 20%
            if sim > 0.2: 
                candidates.append({
                    "Movie": other_movie,
                    "Score": round(sim * 10, 2), # Scale điểm cho đẹp
                    "Reason": f"Nội dung giống {round(sim*100)}%"
                })
        
        # Sắp xếp
        candidates.sort(key=lambda x: x['Score'], reverse=True)
        return candidates[:top_k]

# --- CHẠY TEST ---
if __name__ == "__main__":
    rec = HybridRecommender()
    
    # 1. Test phim phổ biến (Sẽ dùng Luật + Nội dung)
    # Kỳ vọng: Sẽ gợi ý phim sâu sắc hơn Dumb & Dumber
    print(rec.recommend("The Godfather (1972)"))
    
    # 2. Test phim từng bị lỗi rỗng (Sẽ dùng Content Only)
    # Kỳ vọng: Ra Iron Man 2, Avengers...
    print(rec.recommend("The Lion King (1994)"))
    
    # 3. Test phim hành động (Kiểm tra xem có bị lái