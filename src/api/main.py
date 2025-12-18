from fastapi import FastAPI
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

@app.get("/")
def serve_home():
    return {"message": "Welcome to the Movie Recommender System API. Visit /docs for API documentation."}