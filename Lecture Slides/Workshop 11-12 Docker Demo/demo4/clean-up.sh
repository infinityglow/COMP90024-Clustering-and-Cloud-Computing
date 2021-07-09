#!/usr/bin/env zsh

docker-machine ssh manager docker service rm nginx

docker-machine ssh worker1 docker swarm leave

docker-machine ssh worker2 docker swarm leave

docker-machine ssh manager docker swarm leave --force

clear

docker-machine ssh manager docker node ls
