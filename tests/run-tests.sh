#!/bin/bash

docker build -t mongoproxy .. || exit 1
docker compose up --detach --renew-anon-volumes
python3 ./run-mongodb-queries.py
result=$?
docker compose ps
docker compose down --timeout=3
exit $result
