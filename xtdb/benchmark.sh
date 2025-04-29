#!/bin/bash

set -eux

sudo apt install docker.io
sudo apt-get update && sudo apt-get install -y --no-install-recommends \
    curl \
    openjdk-11-jdk \
    rlwrap \
    postgresql-client \
    && sudo apt-get clean \
    && sudo rm -rf /var/lib/apt/lists/*

curl -L -O https://github.com/clojure/brew-install/releases/latest/download/linux-install.sh && \
    chmod +x linux-install.sh && \
    sudo ./linux-install.sh && \
    rm linux-install.sh

# wget --continue 'https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz'
# gzip -d hits.tsv.gz

sudo docker run --rm -p 5432:5432 -v data:/var/lib/xtdb --name xtdbcontainer ghcr.io/xtdb/xtdb > /dev/null 2>&1 &
sleep 30

# psql -h localhost -U xtdb -t

time ./load.sh

# COPY 99997497
# Time: ...

# rm log.txt

./run.sh 2>&1 | tee log.txt

sudo du -bcs /var/lib/docker/volumes/data

cat log.txt | grep -oP 'Time: \d+\.\d+ ms' | sed -r -e 's/Time: ([0-9]+\.[0-9]+) ms/\1/' |
    awk '{ if (i % 3 == 0) { printf "[" }; printf $1 / 1000; if (i % 3 != 2) { printf "," } else { print "]," }; ++i; }'
