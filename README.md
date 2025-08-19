# TICI Benchmark

## Install Dataset

```
wget https://quickwit-datasets-public.s3.amazonaws.com/hdfs-logs-multitenants.json.gz
gzip -d ${ASSET_DIR}/hdfs-logs-multitenants.json.gz
```

## Setup TiUP Mirror

```
tiup mirror set http://tiup.pingcap.net:8988
```

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
python main.py
```
