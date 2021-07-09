#!/usr/bin/env zsh

echo ""
echo "docker-machine ssh manager docker service create --replicas 3 -p 8083:80 --name nginx nginx:alpine"
read \?""
docker-machine ssh manager docker service create --replicas 3 -p 8083:80 --name nginx nginx:alpine

read \?""
open -na "Google Chrome" --args --incognito "http://192.168.99.100:8083" "http://192.168.99.101:8083" "http://192.168.99.102:8083"

clear
