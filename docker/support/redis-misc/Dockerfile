from redis:alpine
COPY mini-redis.conf /etc/redis/redis.conf
#COPY full-redis.conf /etc/redis/redis.conf
RUN  chown redis:redis /etc/redis/redis.conf

VOLUME [ "/data", "/var/log/redis" ]
CMD [ "redis-server", "/etc/redis/redis.conf"  ]
EXPOSE 6379/tcp
