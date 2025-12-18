import streamlit as st
import sys
import os
import pandas as pd
import random

# --- CẤU HÌNH TRANG (Phải đặt đầu tiên) ---
st.set_page_config(
    page_title="CinemAI Recommender",
    page_icon="🍿",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- CẤU HÌNH ĐƯỜNG DẪN ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))

if project_root not in sys.path:
    sys.path.append(project_root)

# --- DỮ LIỆU MẪU ---
MOCK_DATA = {
    "user_recommendations": [
        {"movie": "The Shawshank Redemption", "year": "1994", "genres": "Drama", "score": 4.9, "votes": 45231, "poster": "https://m.media-amazon.com/images/M/MV5BNDE3ODUxMDMyOV5BMl5BanBnXkFtZTgwAzg3MzY0NzE@._V1_SX300.jpg"},
        {"movie": "The Dark Knight", "year": "2008", "genres": "Action | Crime", "score": 4.8, "votes": 38992, "poster": "https://m.media-amazon.com/images/M/MV5BMTMxNTMwODM0NF5BMl5BanBnXkFtZTcwODAyMTk2Mw@@._V1_SX300.jpg"},
        {"movie": "Pulp Fiction", "year": "1994", "genres": "Crime | Drama", "score": 4.7, "votes": 35421, "poster": "https://m.media-amazon.com/images/M/MV5BNGNhMDIzZTUtNTBlZi00MTRlLWFjM2ItYzViMjE3YzI5MjljXkEyXkFqcGdeQXVyNzkwMjQ5NzM@._V1_SX300.jpg"},
        {"movie": "Forrest Gump", "year": "1994", "genres": "Romance", "score": 4.6, "votes": 33210, "poster": "https://m.media-amazon.com/images/M/MV5BNWIwODRlZTUtY2U3ZS00Yzg1LWJhNzYtMmZiYmEyNmU1NjMzXkEyXkFqcGdeQXVyMTQxNzMzNDI@._V1_SX300.jpg"},
        {"movie": "Inception", "year": "2010", "genres": "Sci-Fi", "score": 4.6, "votes": 31998, "poster": "https://m.media-amazon.com/images/M/MV5BMjAxMzY3NjcxNF5BMl5BanBnXkFtZTcwNTI5OTM0Mw@@._V1_SX300.jpg"},
        {"movie": "Interstellar", "year": "2014", "genres": "Sci-Fi | Adventure", "score": 4.5, "votes": 29543, "poster": "https://m.media-amazon.com/images/M/MV5BZjdkOTU3MDktN2IxOS00OGEyLWFmMjktY2FiMmZkNWIyODZiXkEyXkFqcGdeQXVyMTMxODk2OTU@._V1_SX300.jpg"},
        {"movie": "Fight Club", "year": "1999", "genres": "Drama", "score": 4.4, "votes": 28765, "poster": "https://m.media-amazon.com/images/M/MV5BMmEzNTkxYjQtZTc0MC00YTVjLTg5ZTEtZWMwOWVlYzY0NWIwXkEyXkFqcGdeQXVyNzkwMjQ5NzM@._V1_SX300.jpg"},
        {"movie": "The Matrix", "year": "1999", "genres": "Sci-Fi", "score": 4.4, "votes": 30876, "poster": "https://m.media-amazon.com/images/M/MV5BNzQzOTk3OTAtNDQ0Zi00ZTVkLWI0MTEtMDllZjNkYzNjNTc4L2ltYWdlXkEyXkFqcGdeQXVyNjU0OTQ0OTY@._V1_SX300.jpg"},
        {"movie": "Goodfellas", "year": "1990", "genres": "Crime", "score": 4.3, "votes": 27432, "poster": "https://m.media-amazon.com/images/M/MV5BY2NkZjEzMDgtN2RjYy00YzM1LWI4ZmQtMjIwYjFjNmI3ZGEwXkEyXkFqcGdeQXVyNzkwMjQ5NzM@._V1_SX300.jpg"},
        {"movie": "Silence of the Lambs", "year": "1991", "genres": "Thriller", "score": 4.2, "votes": 26198, "poster": "https://m.media-amazon.com/images/M/MV5BNjNhZTk0ZmEtNjJhMi00YzFlLWE1MmEtYzM1M2ZmMGMwMTU4XkEyXkFqcGdeQXVyNjU0OTQ0OTY@._V1_SX300.jpg"},
    ],
    "similar_movies": {
        "Toy Story (1995)": [
            {"movie": "Toy Story 2", "score": 0.92, "genres": "Animation"},
            {"movie": "Finding Nemo", "score": 0.85, "genres": "Adventure"},
            {"movie": "Monsters, Inc.", "score": 0.83, "genres": "Comedy"},
            {"movie": "Shrek", "score": 0.80, "genres": "Comedy"},
            {"movie": "A Bug's Life", "score": 0.78, "genres": "Animation"},
        ]
    }
}

# --- KHỞI TẠO MODEL ---
USE_MOCK_DATA = True 

@st.cache_resource
def load_models():
    if USE_MOCK_DATA:
        return None, None
    try:
        from models.model_2_als.infer import ALSRecommender
        from models.model_1_rules.infer import AssociationRecommender
        return ALSRecommender(), AssociationRecommender()
    except Exception as e:
        st.error(f"Lỗi load model: {e}")
        return None, None

als_model, rules_model = load_models()

# --- CSS CAO CẤP (Dark Cinematic Theme) ---
st.markdown("""
    <style>
    /* NHẬP FONT */
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;600;700&display=swap');

    /* GLOBAL THEME */
    .stApp {
        background-color: #0f172a;
        background-image: radial-gradient(at 0% 0%, hsla(253,16%,7%,1) 0, transparent 50%), 
                          radial-gradient(at 50% 0%, hsla(225,39%,30%,1) 0, transparent 50%), 
                          radial-gradient(at 100% 0%, hsla(339,49%,30%,1) 0, transparent 50%);
        font-family: 'Poppins', sans-serif;
        color: #e2e8f0;
    }

    /* HEADER */
    .main-header {
        text-align: center;
        padding: 2rem 0;
        animation: fadeIn 1.5s ease-in-out;
    }
    .main-header h1 {
        background: linear-gradient(to right, #c084fc, #6366f1, #3b82f6);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 3.5rem;
        font-weight: 800;
        margin-bottom: 0.5rem;
        letter-spacing: -1px;
    }
    .main-header p {
        color: #94a3b8;
        font-size: 1.1rem;
    }

    /* INPUT CONTAINERS */
    .control-panel {
        background: rgba(30, 41, 59, 0.7);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255,255,255,0.1);
        padding: 1.5rem;
        border-radius: 16px;
        margin-bottom: 2rem;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
    }

    /* MOVIE CARD (TAB 1) */
    .movie-card {
        background: rgba(30, 41, 59, 0.4);
        border-radius: 12px;
        overflow: hidden;
        border: 1px solid rgba(255,255,255,0.05);
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        height: 100%;
        position: relative;
    }
    .movie-card:hover {
        transform: translateY(-8px);
        box-shadow: 0 20px 25px -5px rgba(0, 0, 0, 0.5), 0 10px 10px -5px rgba(0, 0, 0, 0.2);
        border-color: #6366f1;
    }
    .movie-poster {
        width: 100%;
        aspect-ratio: 2/3;
        object-fit: cover;
        transition: transform 0.5s ease;
    }
    .movie-card:hover .movie-poster {
        transform: scale(1.05);
    }
    .movie-info {
        padding: 1rem;
    }
    .movie-title {
        font-weight: 700;
        font-size: 1rem;
        color: #f8fafc;
        margin-bottom: 0.25rem;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .movie-meta {
        font-size: 0.8rem;
        color: #94a3b8;
        display: flex;
        justify-content: space-between;
        align-items: center;
    }
    .score-badge {
        background: linear-gradient(135deg, #f59e0b, #d97706);
        color: white;
        padding: 2px 8px;
        border-radius: 6px;
        font-weight: 700;
        font-size: 0.75rem;
        box-shadow: 0 2px 4px rgba(245, 158, 11, 0.3);
    }

    /* SIMILAR MOVIE ROW (TAB 2) */
    .similar-row {
        background: rgba(30, 41, 59, 0.4);
        border-radius: 12px;
        padding: 1rem;
        margin-bottom: 1rem;
        display: flex;
        align-items: center;
        border-left: 4px solid #6366f1;
        transition: background 0.2s;
    }
    .similar-row:hover {
        background: rgba(51, 65, 85, 0.6);
    }
    .rank-circle {
        width: 40px;
        height: 40px;
        border-radius: 50%;
        background: #1e293b;
        color: #6366f1;
        display: flex;
        align-items: center;
        justify-content: center;
        font-weight: 800;
        margin-right: 1rem;
        border: 2px solid #6366f1;
    }

    /* CUSTOMIZE STREAMLIT ELEMENTS */
    .stButton>button {
        background: linear-gradient(90deg, #4f46e5, #7c3aed);
        color: white;
        border: none;
        border-radius: 8px;
        font-weight: 600;
        padding: 0.5rem 2rem;
        transition: opacity 0.2s;
    }
    .stButton>button:hover {
        opacity: 0.9;
        box-shadow: 0 0 15px rgba(99, 102, 241, 0.5);
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 20px;
        background: transparent;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        background-color: transparent;
        border-bottom: 2px solid transparent;
        color: #94a3b8;
    }
    .stTabs [aria-selected="true"] {
        background-color: transparent;
        border-bottom: 2px solid #6366f1;
        color: #f8fafc;
        font-weight: bold;
    }
    </style>
""", unsafe_allow_html=True)

# --- HEADER SECTION ---
st.markdown("""
    <div class="main-header">
        <h1>CINEMAI</h1>
        <p>Khám phá thế giới điện ảnh qua lăng kính Trí tuệ nhân tạo</p>
    </div>
""", unsafe_allow_html=True)

if USE_MOCK_DATA:
    st.warning("⚠️ Đang chạy chế độ Demo (Mock Data)", icon="👾")

# --- MAIN CONTENT ---
tab1, tab2 = st.tabs(["🧩 Gợi ý Cho Bạn", "🔗 Phim Tương Tự"])

# === TAB 1: USER RECOMMENDATION ===
with tab1:
    # Control Panel Container
    st.markdown('<div class="control-panel">', unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 1, 2])
    with c1:
        user_id = st.number_input("User ID", min_value=1, value=123, help="Nhập ID người dùng")
    with c2:
        top_k = st.selectbox("Số lượng phim", [5, 10, 20], index=1)
    with c3:
        st.markdown("<br>", unsafe_allow_html=True)
        btn_get_recs = st.button("🚀 Phân Tích & Gợi Ý", type="primary", use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

    if btn_get_recs:
        # Giả lập loading
        with st.spinner("🧠 AI đang phân tích sở thích của bạn..."):
            # Logic lấy dữ liệu
            if USE_MOCK_DATA:
                recs = MOCK_DATA["user_recommendations"][:top_k]
            else:
                recs = als_model.recommend_for_user(user_id, top_k=top_k) if als_model else []
            
            # Hiển thị
            st.markdown(f"### ✨ Top {len(recs)} phim dành riêng cho User #{user_id}")
            st.markdown("<br>", unsafe_allow_html=True)

            # Grid Layout Custom
            cols_per_row = 5
            for i in range(0, len(recs), cols_per_row):
                cols = st.columns(cols_per_row)
                for j in range(cols_per_row):
                    if i + j < len(recs):
                        movie = recs[i+j]
                        with cols[j]:
                            # HTML Card
                            poster_url = movie.get('poster', 'https://via.placeholder.com/300x450?text=No+Image')
                            st.markdown(f"""
                                <div class="movie-card">
                                    <img src="{poster_url}" class="movie-poster">
                                    <div class="movie-info">
                                        <div class="movie-title" title="{movie['movie']}">{movie['movie']}</div>
                                        <div class="movie-meta">
                                            <span>{movie.get('genres', '').split('|')[0]}</span>
                                            <span class="score-badge">★ {movie['score']}</span>
                                        </div>
                                        <div style="font-size:0.7rem; color:#64748b; margin-top:5px;">
                                            📅 {movie.get('year', 'N/A')} • 👁 {movie.get('votes', 0):,}
                                        </div>
                                    </div>
                                </div>
                            """, unsafe_allow_html=True)

# === TAB 2: ITEM TO ITEM ===
with tab2:
    st.markdown('<div class="control-panel">', unsafe_allow_html=True)
    col_input, col_btn = st.columns([3, 1])
    with col_input:
        movie_list = list(MOCK_DATA["similar_movies"].keys()) if USE_MOCK_DATA else ["Toy Story (1995)", "The Matrix (1999)"]
        selected_movie = st.selectbox("🎬 Chọn phim bạn đã thích:", movie_list)
    with col_btn:
        st.markdown("<br>", unsafe_allow_html=True)
        btn_sim = st.button("🔎 Tìm Phim Giống", use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

    if btn_sim:
        if USE_MOCK_DATA:
            sim_recs = MOCK_DATA["similar_movies"].get(selected_movie, [])
        else:
            sim_recs = rules_model.recommend(selected_movie) if rules_model else []
        
        if sim_recs:
            st.success(f"Nếu bạn thích **{selected_movie}**, chắc chắn bạn sẽ thích:")
            for idx, m in enumerate(sim_recs, 1):
                # Thanh tiến trình giả lập độ chính xác
                confidence = int(m['score'] * 100)
                st.markdown(f"""
                    <div class="similar-row">
                        <div class="rank-circle">#{idx}</div>
                        <div style="flex-grow:1;">
                            <h4 style="margin:0; color:#f8fafc;">{m['movie']}</h4>
                            <p style="margin:0; font-size:0.85rem; color:#94a3b8;">{m.get('genres', 'Unknown')}</p>
                        </div>
                        <div style="text-align:right; min-width:100px;">
                            <span style="font-weight:bold; color:#6366f1;">{confidence}% Match</span>
                            <div style="width:100%; height:4px; background:#334155; border-radius:2px; margin-top:4px;">
                                <div style="width:{confidence}%; height:100%; background:#6366f1; border-radius:2px;"></div>
                            </div>
                        </div>
                    </div>
                """, unsafe_allow_html=True)
        else:
            st.info("Chưa tìm thấy dữ liệu tương đồng cho phim này.")

# --- FOOTER ---
st.markdown("---")
st.markdown("""
    <div style="text-align: center; color: #475569; padding: 20px;">
        <p>© 2024 CinemAI Project | Powered by <b>Spark MLlib</b> & <b>Streamlit</b></p>
    </div>
""", unsafe_allow_html=True)