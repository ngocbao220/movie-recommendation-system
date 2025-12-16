import pandas as pd
import os

# Đường dẫn file luật đã train
ARTIFACT_PATH = "models/model_1_rules/artifacts/rules.parquet"

class AssociationRecommender:
    def __init__(self):
        self.rules = None
        self.load_model()

    def load_model(self):
        """Load file Parquet vào RAM dưới dạng Pandas DataFrame"""
        if os.path.exists(ARTIFACT_PATH):
            try:
                # Cần cài pyarrow: pip install pyarrow
                self.rules = pd.read_parquet(ARTIFACT_PATH)
                print(f"✅ Model 1: Đã load {len(self.rules)} luật kết hợp.")
            except Exception as e:
                print(f"❌ Lỗi khi load Model 1: {e}")
        else:
            print(f"⚠️ Cảnh báo: Chưa tìm thấy file luật tại {ARTIFACT_PATH}. Hãy chạy train.py trước.")

    def recommend(self, movie_name, top_k=5):
        """
        Input: Tên phim (VD: 'Toy Story (1995)')
        Output: List các phim gợi ý
        """
        if self.rules is None or self.rules.empty:
            return []

        # Logic tìm kiếm:
        # Tìm các luật mà 'antecedents' (vế trái) có chứa phim đầu vào
        # Lưu ý: Trong dataframe, antecedents là một mảng (list/array)
        
        # Cách lọc: Kiểm tra xem movie_name có nằm trong list antecedents không
        # Lưu ý: Cần xử lý cẩn thận kiểu dữ liệu list trong pandas
        
        # Tạo mask để lọc (Hơi chậm nếu rules > 1 triệu dòng, nhưng ổn với demo)
        def is_in_antecedents(antecedents_list):
            return movie_name in list(antecedents_list)

        # Lọc ra các luật phù hợp
        matched_rules = self.rules[self.rules['antecedents'].apply(is_in_antecedents)]

        if matched_rules.empty:
            return []

        # Sắp xếp kết quả theo 'lift' (độ liên quan) hoặc 'confidence'
        matched_rules = matched_rules.sort_values(by='lift', ascending=False)

        results = []
        for _, row in matched_rules.head(top_k).iterrows():
            # consequents cũng là 1 list, thường chỉ chứa 1 phim
            recs = list(row['consequents'])
            for rec_movie in recs:
                if rec_movie != movie_name: # Tránh gợi ý lại chính nó
                    results.append({
                        "movie": rec_movie,
                        "score": round(row['lift'], 2), # Dùng Lift làm điểm số
                        "type": "Association Rule"
                    })
                    
        # Khử trùng lặp (nếu có)
        unique_results = []
        seen = set()
        for res in results:
            if res['movie'] not in seen:
                unique_results.append(res)
                seen.add(res['movie'])
                
        return unique_results[:top_k]

# --- Test nhanh khi chạy trực tiếp file này ---
if __name__ == "__main__":
    # Test thử
    rec = AssociationRecommender()
    movie = "Toy Story (1995)" # Thay tên phim có thật trong data của bạn
    print(f"Gợi ý cho '{movie}':")
    print(rec.recommend(movie))