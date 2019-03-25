DOCKER_TAG=python37:latest
DOCKER_NAME=twitter-scraper

SERVICE=19006

# base how to set-up docker base on GIT_REPO variable
TMP_DIR=tmp-git
GIT_REPO= #https://github.com/deeso/fiery-snap.git
BASE_DIR=../../../

# cleaup Docker
docker kill $DOCKER_NAME
docker rm $DOCKER_NAME
rm -fr config.toml python_cmd.sh main.py package $TMP_DIR

if [ ! -z "$GIT_REPO" ] 
then
    git clone $GIT_REPO $TMP_DIR
    BASE_DIR=$TMP_DIR
fi

CONFIGS_DIR=$BASE_DIR/configs/
MAINS_DIR=$BASE_DIR/mains/

CONF_FILE=$CONFIGS_DIR/config.toml
MAIN=$MAINS_DIR/run-all-multiprocess.py
HOST_FILE=$CONFIGS_DIR/hosts

DOCKER_ADD_HOST=
if [ ! -z "$HOST_FILE" ] 
then
    MONGODB_HOST=$(cat $HOST_FILE | grep "mongodb-host")
    REDIS_QUEUE_HOST=$(cat $HOST_FILE | grep "redis-queue-host")
    DOCKER_ADD_HOST=" --add-host $MONGODB_HOST --add-host $REDIS_QUEUE_HOST "    
fi

cp $MAIN main.py
cp $CONF_FILE config.toml
# hack
mkdir package
cp -r $BASE_DIR/setup.py package/
cp -r $BASE_DIR/src package/
cp -r $BASE_DIR/requirements.txt package/

# setup dirs
DOCKER_BASE=/data
DOCKER_NB=$DOCKER_BASE/$DOCKER_NAME
DOCKER_LOGS=$DOCKER_NB/logs
DOCKER_DATA=$DOCKER_NB/data

DOCKER_PORTS="-p $SERVICE:$SERVICE"
DOCKER_ENV=""
DOCKER_VOL=""

mkdir -p $DOCKER_DATA
mkdir -p $DOCKER_LOGS
chmod -R a+rw $DOCKER_NB

# TODO comment below if you want to save to Mongo
echo "python main.py -config config.toml -name $DOCKER_NAME" > python_cmd.sh


cat python_cmd.sh

#docker build --no-cache -t $DOCKER_TAG .
docker build -t $DOCKER_TAG .

# clean up here
rm -fr config.toml python_cmd.sh main.py package $TMP_DIR

# run command not 
echo "docker run $DOCKER_PORTS $DOCKER_VOL -it $DOCKER_ENV \
           --name $DOCKER_NAME $DOCKER_TAG"

docker run -d $DOCKER_ADD_HOST $DOCKER_PORTS $DOCKER_VOL -it $DOCKER_ENV \
           --name $DOCKER_NAME $DOCKER_TAG
