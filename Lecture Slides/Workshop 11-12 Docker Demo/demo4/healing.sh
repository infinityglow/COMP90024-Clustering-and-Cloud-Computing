#!/usr/bin/env zsh

# function pause(){
#   read -p "$*"
# }

echo ""
echo "docker-machine ssh worker1 docker ps -a"
echo ""
docker-machine ssh worker1 docker ps -a

# pause 'Press [Enter] key to continue...'
read \?""
echo ""
echo "docker-machine ssh worker1 docker stop \$(docker-machine ssh worker1 docker ps -aq)"
read \?""
docker-machine ssh worker1 docker stop $(docker-machine ssh worker1 docker ps -aq)

# pause 'Press [Enter] key to continue...'
echo ""
echo "docker-machine ssh worker1 docker ps -a"
echo ""
docker-machine ssh worker1 docker ps -a

sleep 5

echo ""
echo "docker-machine ssh worker1 docker ps -a"
echo ""
docker-machine ssh worker1 docker ps -a
