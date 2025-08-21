# TICI Benchmark

## Setup Minio locally

First, run Minio in a Docker container:

```bash
sudo docker run -d -p 9000:9000 -p 9001:9001 \
  -v /mnt/data:/data \
  quay.io/minio/minio server /data --console-address ":9001"

ContainerID=$(sudo docker ps -q --filter ancestor=quay.io/minio/minio)

sudo docker exec $ContainerID mc alias set myminio http://localhost:9000 minioadmin minioadmin
sudo docker exec $ContainerID mc mb myminio/logbucket
```

## Run the Benchmark

```bash
make test
# or
make test-multi-tiflash
```
