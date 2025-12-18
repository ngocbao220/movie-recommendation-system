import { motion } from 'framer-motion'; // Thêm dòng này
import React from 'react';
import { FaPlay, FaHeart, FaInfoCircle, FaChevronRight, FaRobot } from 'react-icons/fa';

const Hero = ({ movies, userName, onMovieSelect }) => {
  if (!movies || movies.length === 0) {
    return (
      <div className="h-[550px] w-full bg-[#161b22] rounded-[40px] flex items-center justify-center">
        <div className="text-center">
          <FaRobot className="text-yellow-500 text-6xl mx-auto mb-4 animate-bounce" />
          <p className="text-gray-400 font-bold">AI đang tạo danh sách phim dành riêng cho bạn...</p>
        </div>
      </div>
    );
  }

  const mainMovie = movies[0]; // Phim khớp nhất từ ALS
  const thumbnails = movies.slice(1, 5);

  return (
    <section className="text-white w-full">
      {/* Banner AI Personalization */}
      <div className="relative h-[550px] w-full rounded-[40px] overflow-hidden bg-black shadow-2xl border border-white/5">
        <div 
          className="absolute inset-0 bg-cover bg-center"
          style={{ backgroundImage: `url('${mainMovie.backdrop || mainMovie.poster}')` }}
        >
          <div className="absolute inset-0 bg-gradient-to-r from-black via-black/60 to-transparent"></div>
        </div>

        <div className="relative z-10 h-full flex flex-col justify-center px-12 md:px-20 max-w-4xl">
          {/* Badge AI nổi bật */}
          <div className="flex items-center gap-3 mb-6">
             <div className="bg-yellow-500 text-black px-4 py-1 rounded-full font-black text-xs uppercase tracking-widest shadow-[0_0_20px_rgba(245,197,24,0.4)]">
                AI Personalized for User #{userName}
             </div>
             <span className="text-white/50 text-xs font-bold uppercase italic">Model 2: ALS Engine</span>
          </div>

          <h1 className="text-6xl md:text-7xl font-black italic tracking-tighter mb-4 leading-none uppercase">
            {mainMovie.title}
          </h1>
          
          <div className="flex items-center gap-3 mb-6 text-[11px] font-black uppercase">
            <span className="bg-green-500 text-white px-2 py-0.5 rounded shadow-md font-bold">98% MATCH</span>
            <span className="border border-gray-400 px-2 py-0.5 rounded text-gray-200">Dành cho bạn</span>
            <span className="text-gray-300 tracking-widest">{mainMovie.release_date?.split('-')[0]}</span>
          </div>

          <p className="text-gray-300 text-lg leading-relaxed mb-10 line-clamp-3 italic max-w-2xl opacity-80">
            Dựa trên lịch sử xem phim của bạn, hệ thống AI dự đoán bạn sẽ cực kỳ yêu thích bộ phim này.
          </p>

          <div className="flex items-center gap-5">
            <button 
              onClick={() => onMovieSelect(mainMovie.movieId || mainMovie.id)}
              className="px-12 py-5 bg-white text-black font-black text-xl rounded-full hover:bg-yellow-500 transition-all shadow-xl flex items-center justify-center gap-3"
            >
              <FaPlay className="mr-3" /> XEM NGAY
            </button>
            <button className="w-16 h-16 bg-white/10 rounded-full flex items-center justify-center hover:bg-white/20 transition-all border border-white/10 backdrop-blur-md">
              <FaHeart className="text-xl" />
            </button>
          </div>
        </div>

        {/* Danh sách phim gợi ý tiếp theo */}
        <div className="absolute bottom-10 right-10 hidden lg:block">
           <p className="text-right text-[10px] font-black uppercase tracking-[0.3em] text-yellow-500 mb-4">Gợi ý tiếp theo</p>
           <div className="flex gap-4">
              {thumbnails.map((movie, i) => (
                <div 
                  key={i} 
                  onClick={() => onMovieSelect(movie.movieId || movie.id)}
                  className="w-24 h-36 rounded-2xl overflow-hidden border-2 border-white/5 hover:border-yellow-500 cursor-pointer transition-all shadow-2xl group"
                >
                  <img src={movie.poster} alt="thumb" className="w-full h-full object-cover group-hover:scale-110" />
                </div>
              ))}
           </div>
        </div>
      </div>

      {/* Phần Categories giữ nguyên như bạn muốn */}
      {/* Categories Section - Nâng cấp giao diện chuyên nghiệp */}
      <div className="mt-20">
        <div className="flex items-center justify-between mb-10">
          <h3 className="text-2xl font-black text-white uppercase tracking-[0.2em] flex items-center gap-4">
              <span className="w-2 h-10 bg-yellow-500 rounded-full shadow-[0_0_15px_rgba(245,197,24,0.5)]"></span>
              Khám phá theo sở thích
          </h3>
          <div className="text-gray-500 text-xs font-bold uppercase tracking-widest border-b border-gray-800 pb-1 cursor-pointer hover:text-yellow-500 transition-colors">
            Tất cả thể loại
          </div>
        </div>
        
        {/* Grid 7 cột đồng nhất */}
        <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-6">
            {[
              { title: 'Hành Động', icon: '🔥', color: 'from-orange-600/20' },
              { title: 'Viễn Tưởng', icon: '🚀', color: 'from-blue-600/20' },
              { title: 'Kinh Dị', icon: '👻', color: 'from-red-600/20' },
              { title: 'Hài Hước', icon: '🤣', color: 'from-yellow-600/20' },
              { title: 'Lãng Mạn', icon: '💖', color: 'from-pink-600/20' },
              { title: 'Tài Liệu', icon: '📚', color: 'from-emerald-600/20' },
            ].map((cat, i) => (
               <motion.div 
                 whileHover={{ y: -10, scale: 1.02 }}
                 key={i} 
                 className={`relative h-44 rounded-[32px] p-8 overflow-hidden cursor-pointer border border-white/5 bg-gradient-to-br ${cat.color} to-[#161b22] group shadow-xl`}
               >
                  {/* Nội dung chữ */}
                  <div className="relative z-10 h-full flex flex-col justify-between">
                    <span className="text-3xl opacity-50 group-hover:opacity-100 group-hover:scale-125 transition-all duration-500 origin-left">
                      {cat.icon}
                    </span>
                    <h4 className="text-xl font-black text-white leading-tight uppercase tracking-tighter group-hover:text-yellow-500 transition-colors">
                      {cat.title}
                    </h4>
                  </div>

                  {/* Hiệu ứng trang trí phía sau (Glow) */}
                  <div className="absolute -bottom-6 -right-6 w-24 h-24 bg-white/5 rounded-full blur-3xl group-hover:bg-yellow-500/20 transition-all duration-500"></div>
                  
                  {/* Đường kẻ viền chạy khi hover */}
                  <div className="absolute inset-0 border-2 border-yellow-500/0 group-hover:border-yellow-500/20 rounded-[32px] transition-all duration-500"></div>
               </motion.div>
            ))}

            {/* Ô +4 CHỦ ĐỀ - Thiết kế dạng Glassmorphism */}
            <motion.div 
              whileHover={{ y: -10, scale: 1.02 }}
              className="relative h-44 rounded-[32px] flex flex-col items-center justify-center bg-[#1a1a1a] border-2 border-dashed border-gray-800 hover:border-yellow-500/50 transition-all group cursor-pointer"
            >
              <div className="w-12 h-12 rounded-full bg-white/5 flex items-center justify-center mb-3 group-hover:bg-yellow-500 transition-colors">
                <FaChevronRight className="text-white group-hover:text-black transition-colors" />
              </div>
              <span className="text-lg font-black text-gray-400 group-hover:text-white uppercase tracking-tighter">
                  +4 chủ đề
              </span>
              
              {/* Lớp phủ mờ nhẹ */}
              <div className="absolute inset-0 bg-yellow-500/0 group-hover:bg-yellow-500/[0.02] transition-all"></div>
            </motion.div>
        </div>
      </div>
    </section>
  );
};

export default Hero;