import streamlit as st
import requests
import pandas as pd

# --- CẤU HÌNH ---
# Trong Docker, 'api' là tên service của FastAPI định nghĩa trong docker-compose.yml
API_URL = "http://api:8000/api"

st.set_page_config(page_title="Movie Recommender", layout="wide")

# Tùy chỉnh CSS để hiển thị card phim đẹp hơn
st.markdown("""
    <style>
    .movie-card {
        border-radius: 10px;
        background-color: #1e1e1e;
        padding: 10px;
        text-align: center;
    }
    .movie-title {
        font-size: 14px;
        font-weight: bold;
        color: white;
        margin-top: 5px;
        height: 40px;
        overflow: hidden;
    }
    </style>
""", unsafe_allow_html=True)

def fetch_recommendations(user_id):
    try:
        response = requests.get(f"{API_URL}/recommend/user/{user_id}")
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        st.error(f"Lỗi kết nối API: {e}")
    return None

def fetch_movie_detail(movie_id):
    try:
        response = requests.get(f"{API_URL}/movie/{movie_id}")
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        st.error(f"Lỗi: {e}")
    return None

# --- GIAO DIỆN CHÍNH ---
st.title("🎬 Hệ thống gợi ý phim thông minh")

# Sidebar cho việc nhập ID
with st.sidebar:
    st.header("👤 Người dùng")
    user_id = st.number_input("Nhập ID người dùng:", min_value=1, value=1, step=1)
    btn_recommend = st.button("Lấy gợi ý cá nhân hóa")

# Luồng 1: Gợi ý theo User (ALS Model)
if btn_recommend:
    with st.spinner('Đang tính toán gợi ý từ ALS...'):
        movies = fetch_recommendations(user_id)
        
        if movies:
            st.subheader(f"✨ Phim dành riêng cho User {user_id}")
            # Hiển thị dạng lưới 5 cột
            cols = st.columns(5)
            for idx, movie in enumerate(movies):
                with cols[idx % 5]:
                    if movie:
                        if movie['poster']:
                            st.image(movie['poster'], use_column_width=True)
                        st.markdown(f"<div class='movie-title'>{movie['title']}</div>", unsafe_allow_html=True)
                        st.caption(f"⭐ {movie['vote_average']} | 📅 {movie['release_date'][:4]}")
        else:
            st.warning("Không tìm thấy dữ liệu gợi ý cho người dùng này.")

st.divider()

# Luồng 2: Xem chi tiết phim & Phim tương tự (Association Rules)
st.subheader("🔍 Tìm kiếm phim cụ thể")
movie_id_input = st.number_input("Nhập Movie ID để xem chi tiết:", min_value=1, step=1)

if st.button("Xem thông tin phim"):
    data = fetch_movie_detail(movie_id_input)
    
    if data:
        main_movie = data['movie']
        related_movies = data['related']
        
        # Phần 1: Chi tiết phim chính
        col1, col2 = st.columns([1, 2])
        with col1:
            if main_movie['poster']:
                st.image(main_movie['poster'])
        with col2:
            st.header(main_movie['title'])
            st.write(f"**Tên gốc:** {main_movie['original_title']}")
            st.write(f"**Thể loại:** {', '.join(main_movie['genres'])}")
            st.write(f"**Nội dung:** {main_movie['overview']}")
            if main_movie['trailer_key']:
                st.video(f"https://www.youtube.com/watch?v={main_movie['trailer_key']}")
        
        # Phần 2: Phim liên quan (Association Rules)
        st.subheader("🤝 Người xem phim này cũng thích")
        if related_movies:
            r_cols = st.columns(5)
            for idx, r_movie in enumerate(related_movies):
                with r_cols[idx % 5]:
                    if r_movie['poster']:
                        st.image(r_movie['poster'], use_column_width=True)
                    st.markdown(f"<p style='font-size:12px; font-weight:bold;'>{r_movie['title']}</p>", unsafe_allow_html=True)
        else:
            st.info("Chưa có dữ liệu phim liên quan cho phim này.")