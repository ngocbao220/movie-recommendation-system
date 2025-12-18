import httpx
import logging

# Cấu hình logging để dễ theo dõi lỗi khi gọi API
logger = logging.getLogger(__name__)

class TMDBService:
    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.themoviedb.org/3",
        image_base_w300: str = "https://image.tmdb.org/t/p/w300",
        image_base_w500: str = "https://image.tmdb.org/t/p/w500",
        image_base_w1280: str = "https://image.tmdb.org/t/p/w1280",
    ):
        self.api_key = api_key
        self.base_url = base_url
        self.image_base_w300 = image_base_w300
        self.image_base_w500 = image_base_w500
        self.image_base_w1280 = image_base_w1280

    async def get_movie_info(self, client: httpx.AsyncClient, tmdb_id: int):
        """Lấy thông tin cơ bản và hình ảnh của phim"""
        try:
            res = await client.get(
                f"{self.base_url}/movie/{tmdb_id}",
                params={
                    "api_key": self.api_key,
                    "language": "vi-VN",
                    "append_to_response": "images"
                },
                timeout=15.0
            )
            res.raise_for_status()
            return res.json()
        except Exception as e:
            logger.error(f"Lỗi khi lấy thông tin phim {tmdb_id}: {e}")
            return {}

    async def get_trailer_key(self, client: httpx.AsyncClient, tmdb_id: int):
        """Lấy key YouTube của Trailer"""
        try:
            res = await client.get(
                f"{self.base_url}/movie/{tmdb_id}/videos",
                params={
                    "api_key": self.api_key,
                    "language": "en-US" # Trailer thường để en-US sẽ đầy đủ hơn
                },
                timeout=15.0
            )
            res.raise_for_status()
            data = res.json().get("results", [])

            # Ưu tiên lấy Trailer trên YouTube
            for v in data:
                if v["site"] == "YouTube" and v["type"] == "Trailer":
                    return v["key"]
            # Nếu không có Trailer, lấy video YouTube bất kỳ
            for v in data:
                if v["site"] == "YouTube":
                    return v["key"]
            return None
        except Exception as e:
            logger.error(f"Lỗi khi lấy trailer phim {tmdb_id}: {e}")
            return None

    async def get_movie_detail(self, client: httpx.AsyncClient, tmdb_id: int):
        """Hàm tổng hợp để trả về dữ liệu tinh gọn cho Frontend"""
        # Gọi song song 2 API nhỏ để tối ưu thời gian (nếu cần tách biệt)
        # Tuy nhiên ở đây gọi tuần tự vì get_movie_detail cần dữ liệu từ info
        m = await self.get_movie_info(client, tmdb_id)
        if not m:
            return None
            
        trailer_key = await self.get_trailer_key(client, tmdb_id)

        # Trích lọc dữ liệu cần thiết
        return {
            "title": m.get("title"),
            "original_title": m.get("original_title"),
            "release_date": m.get("release_date"),
            "vote_average": m.get("vote_average"),
            "vote_count": m.get("vote_count"),
            "genres": [g["name"] for g in m.get("genres", [])],
            "trailer_key": trailer_key,
            "overview": m.get("overview"),
            "poster": f"{self.image_base_w500}{m['poster_path']}" if m.get("poster_path") else None,
            "backdrop": f"{self.image_base_w1280}{m['backdrop_path']}" if m.get("backdrop_path") else None,
            "logo": (
                f"{self.image_base_w300}{m['images']['logos'][0]['file_path']}"
                if m.get("images", {}).get("logos") else None
            )
        }