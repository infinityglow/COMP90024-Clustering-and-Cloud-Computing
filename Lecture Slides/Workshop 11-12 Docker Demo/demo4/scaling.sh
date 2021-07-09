#!/usr/bin/env zsh

echo ""
echo "docker-machine ssh manager docker service scale nginx=6"
read \?""
docker-machine ssh manager docker service scale nginx=6

read \?""
echo ""
echo "docker-machine ssh manager docker service ls"
echo ""
docker-machine ssh manager docker service ls

read \?""
echo ""
echo "docker-machine ssh manager docker service scale nginx=1"
read \?""
docker-machine ssh manager docker service scale nginx=1

read \?""
echo ""
echo "docker-machine ssh manager docker service ls"
echo ""
docker-machine ssh manager docker service ls
