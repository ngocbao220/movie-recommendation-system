from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from src.api.routes import router as movie_router
import os

app = FastAPI(title="Movie Recommender System")

# 1. Cấu hình CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 2. Kết nối Router
app.include_router(movie_router)

# 3. Mount Static Files (HTML/CSS của bạn)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "../ui")), name="static")

@app.get("/")
def serve_home():
    from fastapi.responses import FileResponse
    return FileResponse(os.path.join(BASE_DIR, "../ui/test.html"))