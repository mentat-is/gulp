FROM node:22-alpine AS build
WORKDIR /app
ARG _VERSION
ENV APP_VERSION=${_VERSION}
RUN corepack enable && corepack prepare pnpm@10.8.1 --activate
COPY package.json pnpm-lock.yaml* ./
RUN pnpm install --frozen-lockfile || pnpm install
COPY . .
RUN node scripts/write-version.js
RUN if pnpm run | grep -qE '^  build'; then pnpm run build; else mkdir -p build && cp -r public/* build/ 2>/dev/null || true; fi

FROM nginx:alpine
COPY --from=build /app/build/ /usr/share/nginx/html/
EXPOSE 80
CMD ["nginx","-g","daemon off;"]
