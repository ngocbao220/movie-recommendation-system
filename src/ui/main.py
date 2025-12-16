import streamlit as st
import sys
import os
import pandas as pd

# --- CẤU HÌNH ĐƯỜNG DẪN ---
# 1. Lấy đường dẫn của file main.py hiện tại
current_dir = os.path.dirname(os.path.abspath(__file__))

# 2. Đi ngược lên 2 cấp để tìm thư mục gốc (src -> ui -> root)
# Cấp 1: src/ui -> src
# Cấp 2: src -> thư mục gốc (nơi chứa folder models)
project_root = os.path.dirname(os.path.dirname(current_dir))

# 3. Thêm thư mục gốc vào hệ thống import của Python
if project_root not in sys.path:
    sys.path.append(project_root)

# Import 2 Class Inference chúng ta đã viết
# Lưu ý: Đảm bảo tên file và class đúng như bạn đã tạo
from models.model_2_als.infer import ALSRecommender
from models.model_1_rules.infer import AssociationRecommender

# --- KHỞI TẠO MODEL (Cache để không load lại mỗi lần f5) ---
@st.cache_resource
def load_models():
    print("🔄 Đang load các model...")
    als = ALSRecommender()
    rules = AssociationRecommender()
    return als, rules

try:
    als_model, rules_model = load_models()
    st.toast("Load model thành công!", icon="✅")
except Exception as e:
    st.error(f"Lỗi load model: {e}")
    st.stop()

# --- GIAO DIỆN ---
st.set_page_config(page_title="Movie Recommender", page_icon="🎬", layout="wide")

st.title("🎬 Hệ thống Gợi ý Phim Thông minh")
st.markdown("---")

# TẠO 2 TAB CHO 2 CHỨC NĂNG
tab1, tab2 = st.tabs(["👤 Gợi ý cho Người dùng (Personalized)", "🔗 Gợi ý theo Phim (Item-to-Item)"])

# === TAB 1: MODEL ALS ===
with tab1:
    st.header("Gợi ý dựa trên Lịch sử xem (Collaborative Filtering)")
    
    col1, col2 = st.columns([1, 3])
    with col1:
        user_id = st.number_input("Nhập User ID:", min_value=1, value=1, step=1)
        btn_user_rec = st.button("Xem Gợi ý cho User này", type="primary")

    if btn_user_rec:
        with st.spinner("Đang phân tích sở thích..."):
            # 1. Hiện lịch sử (Optional - bạn có thể copy code tra lịch sử vào đây nếu muốn)
            st.subheader(f"Dự đoán cho User {user_id}:")
            
            recs = als_model.recommend_for_user(user_id, top_k=10)
            
            if recs:
                # Hiển thị dạng lưới (Grid)
                cols = st.columns(5) # 5 cột mỗi hàng
                for i, movie in enumerate(recs):
                    with cols[i % 5]:
                        # Placeholder ảnh (Sau này có thể thay bằng link poster thật)
                        st.image("https://via.placeholder.com/150x220?text=Movie", use_column_width=True)
                        st.markdown(f"**{movie['movie']}**")
                        st.caption(f"⭐ {movie['score']} | 👥 {movie.get('votes', 'N/A')}")
                        st.markdown(f"*{movie.get('genres', '')}*")
            else:
                st.warning("User này chưa có dữ liệu hoặc User ID mới.")

# === TAB 2: MODEL RULES ===
with tab2:
    st.header("Gợi ý khi xem một phim cụ thể (Association Rules)")
    
    movie_name = st.selectbox(
        "Chọn một phim bạn thích:",
        ["Toy Story (1995)", "Star Wars: Episode IV - A New Hope (1977)", "Pulp Fiction (1994)", "Matrix, The (1999)"]
    )
    
    if st.button("Tìm phim tương tự"):
        with st.spinner("Đang tìm luật kết hợp..."):
            rules_recs = rules_model.recommend(movie_name, top_k=5)
            
            if rules_recs:
                st.success(f"Khán giả xem **{movie_name}** thường xem cùng:")
                for item in rules_recs:
                    st.write(f"- 🎬 **{item['movie']}** (Độ mạnh: {item['score']})")
            else:
                st.info("Không tìm thấy luật kết hợp đủ mạnh cho phim này (Thử phim phổ biến hơn).")

# --- FOOTER ---
st.markdown("---")
st.caption("Project MovieLens 32M | Powered by Spark ALS & FPGrowth")