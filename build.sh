#!/bin/bash
set -e
function cleanup {
  echo 'Removing Postgres & Mongo'
  docker rm --force mongo
  docker rm --force postgres
}
trap cleanup EXIT
docker run --name mongo -d -h mongo -p 27018:27018 -v $PWD/docker/001_init-rs.js:/001_init-rs.js -v $PWD/docker/mongo.conf:/etc/mongo.conf mongo:3.5.13-jessie docker-entrypoint.sh mongod --bind_ip 0.0.0.0 --port 27018 --replSet singleNodeRepl
docker exec mongo mongo --port 27018 /001_init-rs.js
docker run --name postgres -d -p 5442:5432 -e "POSTGRES_PASSWORD=password" -e "POSTGRES_USER=username" -e "POSTGRES_DB=target" postgres:9.6.5-alpine
docker run --rm -v $PWD:/src --link="mongo" --link="postgres" -e "MONGO_URL=mongodb://mongo:27018" -e "POSTGRES_URL=postgresql://username:password@postgres:5432/target" python:3.6.3-jessie /src/docker/build.sh