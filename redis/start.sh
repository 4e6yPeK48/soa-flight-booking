#!/bin/sh
set -eu

if [ "$REDIS_MODE" = sentinel ]; then
    config=/data/sentinel.conf
    if [ ! -f "$config" ]; then
        cat > "$config" <<EOF
port 26379
bind 0.0.0.0
protected-mode no
dir /data
sentinel resolve-hostnames yes
sentinel announce-hostnames yes
sentinel announce-ip $SENTINEL_HOSTNAME
sentinel monitor flights redis-master 6379 2
sentinel down-after-milliseconds flights 3000
sentinel failover-timeout flights 15000
sentinel parallel-syncs flights 1
EOF
    fi
    set -- redis-server "$config" --sentinel
else
    config=/data/redis.conf
    if [ ! -f "$config" ]; then
        cat > "$config" <<EOF
port 6379
bind 0.0.0.0
protected-mode no
dir /data
appendonly yes
replica-announce-ip $REDIS_HOSTNAME
EOF
        if [ "$REDIS_MODE" = replica ]; then
            echo 'replicaof redis-master 6379' >> "$config"
        fi
    fi
    set -- redis-server "$config"
fi
# Sentinel and Redis rewrite these files when roles change; retain them on restart.
chown -R redis:redis /data
exec /usr/local/bin/docker-entrypoint.sh "$@"
