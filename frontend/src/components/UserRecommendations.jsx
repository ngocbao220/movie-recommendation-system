import { useState, useEffect } from 'react'
import { motion } from 'framer-motion'
import axios from 'axios'
import MovieCard from './MovieCard'
import { FaSpinner } from 'react-icons/fa'

function UserRecommendations({ userId }) {
  const [movies, setMovies] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    fetchRecommendations()
  }, [userId])

  const fetchRecommendations = async () => {
    try {
      setLoading(true)
      setError(null)
      const response = await axios.get(`/api/recommend/user/${userId}`)
      setMovies(response.data)
    } catch (err) {
      setError(err.response?.data?.detail || 'Không thể tải dữ liệu gợi ý')
    } finally {
      setLoading(false)
    }
  }

  if (loading) {
    return (
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        className="flex flex-col items-center justify-center py-20"
      >
        <FaSpinner className="text-6xl text-purple-500 animate-spin mb-4" />
        <p className="text-gray-400">Đang tìm phim dành cho bạn...</p>
      </motion.div>
    )
  }

  if (error) {
    return (
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        className="bg-red-900/20 border border-red-500/50 rounded-2xl p-8 mt-8 text-center"
      >
        <p className="text-red-400 text-lg">❌ {error}</p>
      </motion.div>
    )
  }

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      className="mt-12"
    >
      <div className="bg-gradient-to-r from-purple-900/30 to-pink-900/30 rounded-2xl p-6 mb-6 border border-purple-500/20">
        <h3 className="text-2xl font-bold mb-2">
          ✨ Phim dành riêng cho User #{userId}
        </h3>
        <p className="text-gray-400">
          {movies.length} bộ phim được gợi ý dựa trên sở thích của bạn
        </p>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-6">
        {movies.map((movie, index) => (
          <MovieCard key={movie.title + index} movie={movie} index={index} />
        ))}
      </div>
    </motion.div>
  )
}

export default UserRecommendations
