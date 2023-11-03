#!/bin/sh

TAG_CUDA=$1
message=$2

docker images
docker build -t wt358/cuda:$TAG_CUDA -f ./Dockerfile/gpu-Dockerfile ./
docker push wt358/cuda:$TAG_CUDA
docker rmi wt358/cuda:$TAG_CUDA

git add .
git commit -m "$2"
git push origin main
