FROM node:14.11

WORKDIR /usr/src/app

ARG NPM_TOKEN

COPY .npmrcprod .npmrc

COPY package*.json ./

RUN npm install

COPY . .

RUN bash movecreds

EXPOSE 80

CMD ["node","app.js"]