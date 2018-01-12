#!/bin/bash
MASTER_CONFIG=master-config.toml

if [ -n "$1" ]
  then
    MASTER_CONFIG=$1
fi

# create configs
chmod +x prep_configs.sh
./prep_configs.sh $MASTER_CONFIG

# create dockers
cd ../docker/
chmod +x setup_everything.sh
./setup_everything.sh
cd ../scripts

# done
