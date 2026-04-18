FROM node:22-alpine AS build

WORKDIR /web

COPY apps_sdk/web/package.json ./package.json
COPY apps_sdk/web/package-lock.json ./package-lock.json
RUN npm ci

COPY apps_sdk/web ./
RUN npm run build


FROM nginx:1.27-alpine

COPY apps_sdk/web/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /web/dist /usr/share/nginx/html

EXPOSE 8080
