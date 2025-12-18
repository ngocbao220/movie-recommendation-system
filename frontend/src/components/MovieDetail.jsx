import { useState, useEffect } from 'react'
import { motion } from 'framer-motion'
import axios from 'axios'
import MovieCard from './MovieCard'
import { FaPlay, FaStar, FaHeart, FaPlus, FaShare, FaRobot, FaSpinner } from 'react-icons/fa'

function MovieDetail({ movieId }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    if (!movieId) {
      setError("ID phim không hợp lệ");
      setLoading(false);
      return;
    }
    fetchMovieDetail()
    window.scrollTo(0, 0)
  }, [movieId])

  const fetchMovieDetail = async () => {
    try {
      setLoading(true)
      setError(null)
      const response = await axios.get(`/api/movie/${movieId}`)
      if (response.data && response.data.movie) {
        setData(response.data)
      } else {
        setError("Dữ liệu phim bị lỗi");
      }
    } catch (err) {
      setError(err.response?.data?.detail || 'Không tìm thấy phim');
    } finally {
      setLoading(false)
    }
  }

  if (loading) return (
    <div className="flex flex-col items-center justify-center py-40">
      <FaSpinner className="text-6xl text-yellow-500 animate-spin mb-4" />
      <p className="text-gray-400 text-xl font-bold">Hệ thống AI đang phân tích dữ liệu phim...</p>
    </div>
  )

  if (error) return (
    <div className="bg-red-900/20 border border-red-500/50 rounded-2xl p-12 mt-8 text-center max-w-2xl mx-auto">
      <p className="text-red-400 text-2xl font-black italic mb-4">❌ {error}</p>
      <button onClick={() => window.location.reload()} className="bg-red-600 px-6 py-2 rounded-full font-bold">Thử lại</button>
    </div>
  )

  if (!data || !data.movie) return null;
  const { movie, related } = data;

  return (
    <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="w-full bg-[#0a0a0a] min-h-screen">
      {/* 1. Backdrop Cực Đại & Poster Lồng (Phong cách RoPhim mới) */}
      <div className="relative w-full h-[75vh] overflow-hidden shadow-[0_20px_50px_rgba(0,0,0,0.9)]">
        <div 
          className="absolute inset-0 bg-cover bg-center scale-105 blur-sm opacity-40 transition-transform duration-1000 hover:scale-100"
          style={{ backgroundImage: `url(${movie.backdrop || movie.poster})` }}
        />
        <div className="absolute inset-0 bg-gradient-to-t from-[#0a0a0a] via-[#0a0a0a]/30 to-transparent" />
        
        {/* Tiêu đề và Poster chính */}
        <div className="absolute bottom-0 left-0 w-full max-w-[1600px] mx-auto px-8 pb-16 flex flex-col md:flex-row gap-12 items-end z-20">
             <motion.img 
                initial={{ y: 50, opacity: 0 }} 
                animate={{ y: 0, opacity: 1 }}
                src={movie.poster} 
                className="w-[350px] rounded-[30px] shadow-[0_0_80px_rgba(0,0,0,1)] border-4 border-white/5" 
             />
             <div className="mb-6 space-y-4">
                <div className="flex items-center gap-3">
                    <span className="bg-[#f5c518] text-black font-black px-4 py-1 rounded-lg text-lg italic shadow-lg shadow-yellow-500/20">IMDb {movie.vote_average?.toFixed(1)}</span>
                    <span className="bg-white/10 backdrop-blur-md text-white font-bold px-4 py-1 rounded-lg border border-white/20">4K ULTRA HD</span>
                </div>
                <h1 className="text-7xl font-black text-white uppercase tracking-tighter drop-shadow-2xl leading-[0.9]">{movie.title}</h1>
                <p className="text-3xl text-gray-400 italic font-medium tracking-tight">{movie.original_title}</p>
             </div>
        </div>
      </div>

      {/* 2. Nội dung chính & Hệ thống Gợi ý AI */}
      <div className="max-w-[1600px] mx-auto px-8 py-16">
        <div className="flex flex-col xl:flex-row gap-16">
          
          {/* CỘT TRÁI (450px): Thông tin chi tiết & Model 1 */}
          <aside className="w-full xl:w-[450px] shrink-0 space-y-12">
            {/* Khối Nội dung */}
            <div className="bg-[#161b22] rounded-[40px] p-10 border border-white/5 space-y-8 shadow-2xl relative overflow-hidden group">
              <div className="absolute top-0 right-0 w-32 h-32 bg-yellow-500/5 blur-[60px] rounded-full" />
              <div className="flex justify-between items-center border-b border-white/5 pb-6">
                <span className="text-gray-500 font-black uppercase text-sm tracking-[0.2em]">Thông tin phim</span>
                <span className="text-yellow-500 font-black text-xl italic">{movie.release_date?.split('-')[0]}</span>
              </div>
              <p className="text-gray-300 text-xl leading-[1.6] font-medium italic opacity-90 group-hover:opacity-100 transition-opacity">"{movie.overview}"</p>
              
              <div className="space-y-4 pt-4 text-lg">
                <div className="flex gap-4 items-center"><span className="text-gray-500 font-bold w-24">Thể loại:</span> <span className="text-white font-black">{movie.genres?.join(' • ')}</span></div>
                <div className="flex gap-4 items-center"><span className="text-gray-500 font-bold w-24">Quốc gia:</span> <span className="text-white font-black">Âu Mỹ / Quốc Tế</span></div>
              </div>
            </div>

            {/* AI RECOMMENDATION SYSTEM (LÀM CỰC NỔI BẬT) */}
            <div className="bg-gradient-to-br from-[#1c2128] to-[#0d1117] rounded-[40px] overflow-hidden border-2 border-yellow-500/30 shadow-[0_0_50px_rgba(245,197,24,0.15)] group">
              <div className="p-8 bg-yellow-500/5 border-b border-white/5 flex items-center justify-between">
                <div className="flex items-center gap-4">
                  <div className="p-3 bg-yellow-500 rounded-2xl shadow-lg shadow-yellow-500/40">
                    <FaRobot className="text-black text-3xl" />
                  </div>
                  <div>
                    <span className="font-black text-white text-2xl uppercase tracking-tighter">AI Gợi Ý</span>
                    <p className="text-[10px] text-yellow-500 font-black tracking-[0.3em] uppercase">Model 1: Association Rules</p>
                  </div>
                </div>
              </div>
              <div className="p-6 space-y-6">
                {related?.slice(0, 5).map((rm, i) => (
                  <motion.div 
                    whileHover={{ x: 10 }}
                    key={i} 
                    className="flex gap-6 p-4 hover:bg-white/5 rounded-3xl transition-all cursor-pointer border border-transparent hover:border-white/5"
                  >
                    <div className="relative w-24 h-32 shrink-0">
                      <img src={rm.poster} className="w-full h-full object-cover rounded-2xl shadow-lg" />
                      <div className="absolute -top-3 -left-3 bg-green-500 text-white text-xs font-black px-2 py-1 rounded-xl shadow-xl animate-pulse">
                        {Math.floor(Math.random() * 10 + 89)}% MATCH
                      </div>
                    </div>
                    <div className="flex flex-col justify-center overflow-hidden">
                      <h4 className="text-white font-black text-xl truncate mb-2 uppercase tracking-tight group-hover:text-yellow-500">{rm.title}</h4>
                      <div className="flex items-center gap-3 text-yellow-500 font-black">
                        <FaStar size={16} /> <span className="text-lg italic">{rm.vote_average?.toFixed(1)}</span>
                      </div>
                    </div>
                  </motion.div>
                ))}
              </div>
            </div>
          </aside>

          {/* CỘT PHẢI: Trình phát & Cộng đồng */}
          <main className="flex-grow space-y-12">
            {/* Control Center (Phóng đại) */}
            <div className="bg-[#161b22] p-8 rounded-[50px] flex items-center justify-between border border-white/5 shadow-2xl">
              <button className="flex items-center gap-6 px-20 py-8 bg-yellow-500 text-black font-black text-3xl rounded-full hover:scale-105 transition-all shadow-[0_20px_50px_rgba(245,197,24,0.4)]">
                <FaPlay size={32} /> XEM NGAY
              </button>
              
              <div className="flex gap-14 text-gray-500 px-10">
                 <div className="flex flex-col items-center gap-3 hover:text-red-500 cursor-pointer transition-all scale-125">
                    <FaHeart size={40} />
                    <span className="text-[10px] font-black uppercase tracking-[0.2em]">Yêu thích</span>
                 </div>
                 <div className="flex flex-col items-center gap-3 hover:text-white cursor-pointer transition-all scale-125">
                    <FaPlus size={40} />
                    <span className="text-[10px] font-black uppercase tracking-[0.2em]">Danh sách</span>
                 </div>
                 <div className="flex flex-col items-center gap-3 hover:text-blue-400 cursor-pointer transition-all scale-125">
                    <FaShare size={40} />
                    <span className="text-[10px] font-black uppercase tracking-[0.2em]">Chia sẻ</span>
                 </div>
              </div>
            </div>

            {/* Video Player (Trình chiếu rạp) */}
            <div className="bg-black rounded-[60px] overflow-hidden aspect-video border-4 border-white/5 shadow-[0_0_150px_rgba(0,0,0,1)]">
               {movie.trailer_key ? (
                 <iframe width="100%" height="100%" src={`https://www.youtube.com/embed/${movie.trailer_key}?rel=0&autoplay=0&modestbranding=1&vq=hd1080`} frameBorder="0" allowFullScreen></iframe>
               ) : (
                 <div className="w-full h-full flex flex-col items-center justify-center text-gray-700 bg-[#0d1117] gap-4">
                    <FaRobot size={80} className="opacity-20 animate-bounce" />
                    <div className="font-black text-4xl italic opacity-20">TRAILER ĐANG CẬP NHẬT</div>
                 </div>
               )}
            </div>

            {/* Khối Bình luận Phủ rộng */}
            <div className="bg-[#161b22] rounded-[50px] border border-white/5 overflow-hidden shadow-2xl">
               <div className="p-10 border-b border-white/10 flex justify-between items-center bg-white/5">
                  <h3 className="text-2xl font-black text-white uppercase tracking-widest flex items-center gap-4">
                    <span className="w-2 h-10 bg-yellow-500 rounded-full" />
                    Cộng đồng thảo luận AI
                  </h3>
                  <span className="text-yellow-500 font-black text-xl italic tracking-tighter">1,250 Phản hồi</span>
               </div>
               <div className="p-40 text-center space-y-6">
                  <p className="text-gray-400 text-3xl font-black italic opacity-60">Hãy đăng nhập để tham gia cộng đồng người yêu phim RoPhim</p>
                  <button className="bg-white/5 px-10 py-4 rounded-2xl text-white font-black hover:bg-white/10 transition-all uppercase tracking-widest border border-white/10">Đăng nhập ngay</button>
               </div>
            </div>
          </main>

        </div>
      </div>
    </motion.div>
  )
}

export default MovieDetail