#!/bin/bash

# Stop all the containers at once
docker-compose down

docker stop $(docker ps | awk '{print $1}') 2> /dev/null
docker rm $(docker ps -a | awk '{print $1}') 2> /dev/null
