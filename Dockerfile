# Stage 1: Build NestJS application with Bun
FROM oven/bun:1-alpine AS builder

WORKDIR /usr/src/app

COPY package.json bun.lock* bun.lockb* ./
RUN bun install --frozen-lockfile || bun install

COPY tsconfig*.json nest-cli.json ./
COPY src/ ./src/

RUN bun run build

# Stage 2: Production runtime with Bun
FROM oven/bun:1-alpine AS runner

WORKDIR /usr/src/app
ENV NODE_ENV=production

COPY package.json bun.lock* bun.lockb* ./
RUN bun install --production

COPY --from=builder /usr/src/app/dist ./dist

EXPOSE 3000

CMD ["bun", "dist/main.js"]
