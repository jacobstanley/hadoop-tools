#!/bin/sh -e

OUTPUT=$PWD/dist/centos6
DOCKER_TAG=hadoop-tools

docker build -t $DOCKER_TAG .
docker run   -t $DOCKER_TAG hh --help

mkdir -p $OUTPUT

docker run \
    -v $OUTPUT:/dist:rw \
    -t $DOCKER_TAG \
    cp /root/.cabal/bin/hh /dist/
