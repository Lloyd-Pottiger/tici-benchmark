ASSET_DIR ?= $(shell pwd)/assets

setup:
	mkdir -p ${ASSET_DIR}
	tiup mirror set http://tiup.pingcap.net:8988

install: setup
	head -n 1 ${ASSET_DIR}/hdfs-logs-multitenants.json > /dev/null || \
		(echo "Downloading hdfs-logs-multitenants.json.gz to ${ASSET_DIR}" && \
		wget -q https://quickwit-datasets-public.s3.amazonaws.com/hdfs-logs-multitenants.json.gz -O ${ASSET_DIR}/hdfs-logs-multitenants.json.gz && \
		echo "Decompressing hdfs-logs-multitenants.json.gz" && \
		gzip -d ${ASSET_DIR}/hdfs-logs-multitenants.json.gz && \
		rm -f ${ASSET_DIR}/hdfs-logs-multitenants.json.gz)

test: install
	ASSET_DIR=${ASSET_DIR} python3 main.py

test-multi-tiflash: install
	ASSET_DIR=${ASSET_DIR} python3 main.py --tiflash 2
