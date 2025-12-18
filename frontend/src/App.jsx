import { useState, useEffect } from 'react'
import { motion } from 'framer-motion'
import { FaUser, FaSearch, FaFilm, FaHome, FaFire, FaStar, FaRobot } from 'react-icons/fa'
import axios from 'axios'
import UserRecommendations from './components/UserRecommendations'
import MovieDetail from './components/MovieDetail'
import Hero from './components/Hero'
import MovieCard from './components/MovieCard'

function App() {
  const [activeTab, setActiveTab] = useState('home')
  const [userId, setUserId] = useState(1)
  const [movieId, setMovieId] = useState('')
  const [movieSearch, setMovieSearch] = useState('')
  const [searchResults, setSearchResults] = useState([])
  const [showUserRecs, setShowUserRecs] = useState(false)
  const [showMovieDetail, setShowMovieDetail] = useState(false)
  const [popularMovies, setPopularMovies] = useState([]);
  
  // Bổ sung state đăng nhập và Hero
  const [isLoggedIn, setIsLoggedIn] = useState(false)
  const [loginUserId, setLoginUserId] = useState('')
  const [password, setPassword] = useState('')
  const [heroMovies, setHeroMovies] = useState([])

  // 1. Lấy top phim phổ biến cho trang chủ (Instant, không chạy model)
  useEffect(() => {
    const fetchPopularMovies = async () => {
      try {
        const response = await axios.get('/api/movie/popular/list');
        setPopularMovies(response.data);
      } catch (err) {
        console.error('Lỗi khi lấy phim nổi bật:', err);
      }
    };
    fetchPopularMovies();
  }, []);

  // 2. Xử lý Đăng nhập & Gọi Model 2 cho Hero (Chỉ chạy khi bấm nút)
  const handleLogin = async (e) => {
    e.preventDefault();
    if (loginUserId && password === '1234') {
      const targetId = parseInt(loginUserId);
      setUserId(targetId);
      setIsLoggedIn(true);

      // Gọi API Model 2 ngay lập tức cho Hero
      try {
        console.log(`🔐 Login as User ${targetId}, fetching recommendations...`);
        const response = await axios.get(`/api/recommend/user/${targetId}`);
        console.log('✅ API Response:', response.data.length, 'movies');
        console.log('📽️ First movie:', response.data[0]);
        setHeroMovies(response.data);
      } catch (err) {
        console.error('Lỗi lấy gợi ý Hero:', err);
        setHeroMovies(popularMovies); // Fallback nếu lỗi
      }
    } else {
      alert("Sai mật khẩu (1234)");
    }
  };

  // 3. Tìm kiếm phim (Debounce logic)
  useEffect(() => {
    const searchMovies = async () => {
      if (movieSearch.length < 2) {
        setSearchResults([])
        return
      }
      try {
        const response = await axios.get(`/api/search/movies?q=${encodeURIComponent(movieSearch)}&limit=10`)
        setSearchResults(response.data)
      } catch (err) {
        console.error('Search error:', err)
      }
    }
    const debounce = setTimeout(searchMovies, 300)
    return () => clearTimeout(debounce)
  }, [movieSearch])

  const handleMovieSelect = (selectedMovieId) => {
    setMovieId(selectedMovieId)
    setShowMovieDetail(true)
    setShowUserRecs(false)
    setMovieSearch('')
    setSearchResults([])
    setActiveTab('movie')
  }

  // --- GIAO DIỆN ĐĂNG NHẬP ---
  if (!isLoggedIn) {
    return (
      <div className="min-h-screen bg-[#0a0a0a] flex items-center justify-center p-6">
        <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }}
          className="bg-[#161b22] p-10 rounded-[40px] border border-white/5 w-full max-w-md shadow-2xl"
        >
          <h2 className="text-3xl font-black text-white mb-8 text-center uppercase tracking-tighter">Đăng nhập RoPhim</h2>
          <form onSubmit={handleLogin} className="space-y-6">
            <input 
              type="number" placeholder="Nhập User ID" required
              className="w-full bg-black/50 border border-white/10 p-4 rounded-2xl text-white outline-none focus:border-yellow-500 transition-all"
              value={loginUserId} onChange={(e) => setLoginUserId(e.target.value)}
            />
            <input 
              type="password" placeholder="Mật khẩu" required
              className="w-full bg-black/50 border border-white/10 p-4 rounded-2xl text-white outline-none focus:border-yellow-500 transition-all"
              value={password} onChange={(e) => setPassword(e.target.value)}
            />
            <button type="submit" className="w-full bg-yellow-500 text-black font-black py-4 rounded-2xl hover:scale-105 transition-all uppercase">
              Vào xem phim
            </button>
          </form>
        </motion.div>
      </div>
    );
  }

  // --- GIAO DIỆN CHÍNH ---
  return (
    <div className="min-h-screen bg-[#0a0a0a] text-white w-full overflow-x-hidden">
      <header className="bg-[#0b0f15] border-b border-gray-800 sticky top-0 z-50 w-full">
        <div className="max-w-[1600px] mx-auto px-8">
          <div className="flex items-center h-[75px] gap-6">
            {/* Logo */}
            <div className="flex items-center gap-2 cursor-pointer min-w-max" onClick={() => {setActiveTab('home'); setShowMovieDetail(false)}}>
              <div className="relative">
                <div className="w-10 h-10 border-2 border-yellow-500 rounded-full flex items-center justify-center">
                   <div className="w-0 h-0 border-t-[6px] border-t-transparent border-l-[10px] border-l-white border-b-[6px] border-b-transparent ml-1"></div>
                </div>
                <div className="absolute -bottom-1 left-0 w-full h-2 border-b-2 border-yellow-500 rounded-[50%]"></div>
              </div>
              <h1 className="text-2xl font-extrabold tracking-tight">RoPhim</h1>
            </div>

            {/* Search */}
            <div className="flex-1 max-w-md relative">
              <div className="relative group">
                <input
                  type="text" value={movieSearch} onChange={(e) => setMovieSearch(e.target.value)}
                  placeholder="Tìm kiếm phim, diễn viên..."
                  className="w-full px-11 py-2.5 bg-[#1a202c] border border-transparent focus:border-gray-600 rounded-md text-sm outline-none transition-all"
                />
                <FaSearch className="absolute left-4 top-1/2 -translate-y-1/2 text-gray-500" />
              </div>
              {searchResults.length > 0 && (
                <div className="absolute z-50 w-full mt-1 bg-[#1a202c] border border-gray-700 rounded shadow-xl max-h-96 overflow-y-auto">
                  {searchResults.map((movie) => (
                    <div key={movie.movieId} onClick={() => handleMovieSelect(movie.movieId)} className="px-4 py-2 hover:bg-yellow-500 hover:text-black text-sm font-bold cursor-pointer border-b border-gray-700 last:border-0">{movie.title}</div>
                  ))}
                </div>
              )}
            </div>

            {/* Nav */}
            <nav className="hidden lg:flex items-center gap-6 text-[15px] font-medium text-gray-300">
              <button onClick={() => {setActiveTab('home'); setShowMovieDetail(false)}} className="hover:text-white">Trang chủ</button>
              <button onClick={() => setActiveTab('trending')} className="hover:text-white">Phim hot</button>
              <button onClick={() => setActiveTab('user')} className="hover:text-white">Dành cho bạn</button>
              <div className="flex items-center gap-1 cursor-pointer"><span className="bg-[#f5c518] text-black text-[10px] px-1 rounded font-bold">NEW</span><span className="hover:text-white">Rổ Bóng</span></div>
            </nav>

            <div className="flex items-center gap-4 ml-auto">
              <div className="bg-white/5 px-4 py-2 rounded-full border border-white/10 flex items-center gap-3">
                <FaUser className="text-yellow-500 text-xs" />
                <span className="text-sm font-bold">User #{userId}</span>
              </div>
            </div>
          </div>
        </div>
      </header>

      <main className="w-full">
        {activeTab === 'home' && !showMovieDetail && (
          <div className="w-full flex flex-col items-center">
            {/* Hero phủ rộng */}
            <div className="w-full max-w-[1600px] px-8 mt-6">
               <Hero movies={heroMovies} userName={userId} onMovieSelect={handleMovieSelect} />
            </div>

            {/* Phim Mới Cập Nhật - popularMovies */}
            <section className="w-full max-w-[1600px] px-8 py-12 space-y-10">
              <div className="flex items-center justify-between border-b border-gray-800 pb-4">
                <h2 className="text-3xl font-black flex items-center gap-3">
                  <span className="w-1.5 h-8 bg-yellow-500 rounded-full"></span>
                  Phim Mới Cập Nhật
                </h2>
                <button className="text-sm text-gray-400 hover:text-yellow-500 font-bold uppercase tracking-widest">Xem tất cả</button>
              </div>
              
              <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-6 gap-8">
                {popularMovies.map((movie, index) => (
                  <MovieCard key={index} movie={movie} index={index} onClick={() => handleMovieSelect(movie.id || movie.movieId)} />
                ))}
              </div>
            </section>
          </div>
        )}

        {(activeTab === 'movie' || showMovieDetail) && (
          <div className="w-full">
            <MovieDetail movieId={movieId} />
          </div>
        )}

        {activeTab === 'trending' && (
          <div className="max-w-[1600px] mx-auto px-8 py-10">
            <h2 className="text-3xl font-bold mb-8">🔥 Phim đang thịnh hành</h2>
            <div className="grid grid-cols-2 md:grid-cols-5 gap-6">
              {popularMovies.map((movie, index) => (
                <MovieCard key={index} movie={movie} index={index} onClick={() => handleMovieSelect(movie.id || movie.movieId)} />
              ))}
            </div>
          </div>
        )}

        {activeTab === 'user' && (
          <div className="max-w-[1600px] mx-auto px-8 py-10">
            <UserRecommendations userId={userId} />
          </div>
        )}
      </main>

      <footer className="bg-[#0b0f15] border-t border-gray-800 mt-20 py-12 text-center text-gray-500 w-full">
          <p className="text-white font-black uppercase tracking-widest mb-2">RoPhim Engine</p>
          <p>© 2025 Hệ thống gợi ý phim thông minh với AI & Machine Learning.</p>
      </footer>
    </div>
  )
}

export default App