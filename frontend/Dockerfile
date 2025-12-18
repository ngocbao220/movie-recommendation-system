# Frontend: React + Vite development server
FROM node:20-alpine

WORKDIR /app

# Copy package files
COPY package.json package-lock.json* ./

# Install dependencies với timeout dài hơn
RUN npm config set fetch-timeout 300000 && \
    npm config set fetch-retry-maxtimeout 300000 && \
    npm install --prefer-offline --no-audit

# Copy source code
COPY . .

# Expose Vite dev server port
EXPOSE 3000

# Start development server
CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0", "--port", "3000"]
