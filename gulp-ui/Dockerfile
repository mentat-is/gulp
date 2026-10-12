FROM node:22-alpine AS build
WORKDIR /app
RUN corepack enable && corepack prepare pnpm@10.8.1 --activate
COPY package.json pnpm-lock.yaml* ./
RUN pnpm install --frozen-lockfile || pnpm install
COPY . .
RUN if pnpm run | grep -qE '^  build'; then pnpm run build; else mkdir -p build && cp -r public/* build/ 2>/dev/null || true; fi

FROM nginx:alpine
ARG GULP_VERSION=0.0.0
ARG GULP_REVISION=0000000000000000000000000000000000000000
ARG GULP_SOURCE=https://github.com/mentat-is/gulp
LABEL org.opencontainers.image.version="${GULP_VERSION}" \
      org.opencontainers.image.revision="${GULP_REVISION}" \
      org.opencontainers.image.source="${GULP_SOURCE}"
COPY --from=build /app/build/ /usr/share/nginx/html/
EXPOSE 80
CMD ["nginx","-g","daemon off;"]
