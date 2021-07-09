#!/usr/bin/env zsh

echo ""
echo "docker-machine ssh manager docker service scale nginx=6"
echo ""
docker-machine ssh manager docker service scale nginx=6

read \?""
echo ""
echo "docker-machine ssh manager docker service ps nginx"
read \?""
docker-machine ssh manager docker service ps nginx

read \?""
echo ""
echo "docker-machine ssh manager docker service update --image=alwynpan/comp90024:demo1 nginx"
read \?""
docker-machine ssh manager docker service update --image=alwynpan/comp90024:demo1 nginx

read \?""
echo ""
echo "docker-machine ssh manager docker service ps nginx"
read \?""
docker-machine ssh manager docker service ps nginx
