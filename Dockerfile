FROM node:16-alpine as base

USER root
WORKDIR /webapps
COPY package*.json ./
RUN npm install && \
    npm install -g pm2
COPY . /webapps
RUN chmod -R 777 /webapps/script/

ENV NODE_ENV production
