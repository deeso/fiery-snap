#!/bin/bash
MASTER_CONFIG=master-config.toml

if [ -n "$1" ]
  then
    MASTER_CONFIG=$1
fi

#CONFIGS=("twitter-source.toml" "twitter-scraper.toml" "flatfile-sink.toml" "mongo-sink.toml")
#
#cd ../configs/
#for CONFIG in "${CONFIGS[@]}"
#do
#   echo "cp $MASTER_CONFIG $CONFIG"
#   cp $MASTER_CONFIG $CONFIG
#done
cp $MASTER_CONFIG ../configs/config.toml
cd ../scripts/
