#!/bin/bash

docker-compose up --detach --renew-anon-volumes
python3 ./run-mongodb-queries.py
docker-compose ps
docker-compose down --timeout=3
