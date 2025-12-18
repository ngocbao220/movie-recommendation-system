import pandas as pd
import os

# Đường dẫn file luật đã train - Sử dụng checkpoint có sẵn
ARTIFACT_PATH = "checkpoints/model_1_rulesv3/rules.parquet"

class AssociationRecommender:
    def __init__(self):
        self.rules = None
        self.load_model()

    def load_model(self):
        """Load file Parquet vào RAM dưới dạng Pandas DataFrame"""
        if os.path.exists(ARTIFACT_PATH):
            try:
                self.rules = pd.read_parquet(ARTIFACT_PATH)
                print(f"✅ Model 1: Đã load {len(self.rules)} luật kết hợp.")
                # In ra tên cột để kiểm tra (Debug)
                print(f"   👉 Các cột có trong file: {self.rules.columns.tolist()}")
            except Exception as e:
                print(f"❌ Lỗi khi load Model 1: {e}")
        else:
            print(f"⚠️ Cảnh báo: Chưa tìm thấy file luật tại {ARTIFACT_PATH}. Hãy chạy train.py trước.")

    def recommend(self, movie_name, top_k=10):
        """
        Input: Tên phim (VD: 'Toy Story (1995)')
        Output: List các phim gợi ý
        """
        if self.rules is None or self.rules.empty:
            return []

        # Parquet file có columns: 'antecedent', 'consequent' (không có 's')
        # antecedent và consequent là list của tên phim
        
        # Tìm các luật mà 'antecedent' có chứa phim đầu vào
        def is_in_antecedent(antecedent_list):
            return movie_name in list(antecedent_list)

        # Lọc ra các luật phù hợp
        matched_rules = self.rules[self.rules['antecedent'].apply(is_in_antecedent)]

        if matched_rules.empty:
            return []

        # Sắp xếp kết quả theo 'lift' (độ liên quan)
        matched_rules = matched_rules.sort_values(by='lift', ascending=False)

        results = []
        for _, row in matched_rules.head(top_k).iterrows():
            # consequent cũng là 1 list, thường chỉ chứa 1 phim
            recs = list(row['consequent'])
            for rec_movie in recs:
                if rec_movie != movie_name: 
                    results.append({
                        "movie": rec_movie,
                        "score": round(row['lift'], 2), 
                        "type": "Association Rule"
                    })
                    
        # Khử trùng lặp
        unique_results = []
        seen = set()
        for res in results:
            if res['movie'] not in seen:
                unique_results.append(res)
                seen.add(res['movie'])
                
        return unique_results[:top_k]

if __name__ == "__main__":
    # Test thử
    rec = AssociationRecommender()
    # Bạn có thể đổi tên phim khác để test
    movie = "The Godfather (1972)" 
    print(f"Gợi ý cho '{movie}':")
    print(rec.recommend(movie))