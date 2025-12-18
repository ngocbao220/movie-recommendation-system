import { motion } from 'framer-motion'
import { FaStar, FaPlay } from 'react-icons/fa'

function MovieCard({ movie, index, onClick }) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: index * 0.05 }}
      onClick={onClick} // Thêm sự kiện click vào đây
      className="group relative cursor-pointer"
    >
      {/* Container Ảnh */}
      <div className="relative aspect-[2/3] rounded-xl overflow-hidden bg-[#1a202c] border border-gray-800 transition-all duration-300 group-hover:scale-105 group-hover:border-yellow-500/50 group-hover:shadow-[0_0_20px_rgba(245,197,24,0.2)]">
        {movie.poster ? (
          <img
            src={movie.poster}
            alt={movie.title}
            className="w-full h-full object-cover transition-transform duration-500 group-hover:scale-110"
          />
        ) : (
          <div className="w-full h-full flex items-center justify-center text-4xl">🎬</div>
        )}
        
        {/* Overlay Play khi hover */}
        <div className="absolute inset-0 bg-black/40 opacity-0 group-hover:opacity-100 transition-opacity flex items-center justify-center">
          <div className="w-12 h-12 bg-yellow-500 rounded-full flex items-center justify-center text-black transform scale-50 group-hover:scale-100 transition-transform duration-300">
            <FaPlay className="ml-1" />
          </div>
        </div>

        {/* Nhãn IMDb & Chất lượng */}
        <div className="absolute top-2 left-2 flex flex-col gap-1">
          <span className="bg-yellow-500 text-black text-[10px] font-bold px-1.5 py-0.5 rounded shadow-md">
            {movie.quality || 'HD'}
          </span>
          <span className="bg-black/70 text-white text-[10px] font-bold px-1.5 py-0.5 rounded backdrop-blur-sm border border-white/10 flex items-center gap-1">
            <FaStar className="text-yellow-500 text-[8px]" /> {movie.vote_average?.toFixed(1) || 'N/A'}
          </span>
        </div>
      </div>

      {/* Thông tin phim */}
      <div className="mt-3">
        <h4 className="font-bold text-sm text-gray-200 line-clamp-1 group-hover:text-yellow-500 transition-colors">
          {movie.title}
        </h4>
        <div className="flex items-center justify-between mt-1">
          <span className="text-[11px] text-gray-500">
            {movie.release_date ? movie.release_date.split('-')[0] : '2024'}
          </span>
          <span className="text-[11px] text-gray-400 group-hover:text-gray-300">Phim Lẻ</span>
        </div>
      </div>
    </motion.div>
  )
}

export default MovieCard