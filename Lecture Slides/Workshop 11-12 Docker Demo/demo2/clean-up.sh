#!/usr/bin/env zsh

echo "docker rm -f \$(docker ps -aq)"

docker rm -f $(docker ps -aq)

clear