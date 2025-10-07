# Dockerfile
FROM node:22-bookworm-slim

# Build tools for native modules
RUN apt-get update && apt-get install -y python3 make g++ && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY package*.json ./

# Force native builds to match this image's glibc
ENV npm_config_build_from_source=true

# Clean, reproducible install
RUN npm ci --omit=dev
# Explicitly rebuild better-sqlite3 against this image
RUN npm rebuild better-sqlite3

# Bring in the app
COPY . .

ENV NODE_ENV=production
CMD ["npm","start"]
