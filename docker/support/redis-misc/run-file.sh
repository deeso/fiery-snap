DOCKER_TAG=redis:latest
DOCKER_NAME=redis-misc

# cleaup Docker
docker kill $DOCKER_NAME
docker rm $DOCKER_NAME

SERVICE=6379:6379
GIT_REPO=https://github.office.opendns.com/adpridge/fiery-snap.git
TMP_DIR=tmp-fiery-snap
BASE_DIR=../../../
CONFIGS_DIR=$BASE_DIR/configs/

CONF_FILE=$CONFIGS_DIR/$DOCKER_NAME.toml
HOST_FILE=$CONFIGS_DIR/hosts

MAINS_DIR=$BASE_DIR/mains
MAIN=$MAINS_DIR/$DOCKER_NAME.py

# git clone $GIT_REPO $TMP_DIR

MONGODB_HOST=$(cat $HOST_FILE | grep "mongodb-host")
REDIS_QUEUE_HOST=$(cat $HOST_FILE | grep "redis-queue-host")
DOCKER_ADD_HOST="--add-host $MONGODB_HOST --add-host $REDIS_QUEUE_HOST"
rm -rf $TMP_DIR

# setup dirs 
DOCKER_BASE=/data
DOCKER_NB=$DOCKER_BASE/$DOCKER_NAME
DOCKER_LOGS=$DOCKER_NB/logs
DOCKER_DATA=$DOCKER_NB/data
DOCKER_ENV=''
DOCKER_PORTS="-p $SERVICE "
DOCKER_VOLS="-v $DOCKER_DATA:/data -v $DOCKER_LOGS:/var/log/redis"

# create the required dirs
mkdir -p $DOCKER_DATA
mkdir -p $DOCKER_LOGS
chmod -R a+rw $DOCKER_NB

docker build -t $DOCKER_TAG .
# run command
docker run -d $DOCKER_ADD_HOST $DOCKER_PORTS $DOCKER_VOL -it  $DOCKER_ENV \
           --name $DOCKER_NAME $DOCKER_TAG
