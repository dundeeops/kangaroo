To run etcd

```
docker run -d --restart unless-stopped \
    -p 2379:2379 \
    -p 4001:4001 \
    --name etcd \
    quay.io/coreos/etcd:v2.3.0 \
    -listen-client-urls http://0.0.0.0:2379,http://0.0.0.0:4001 -advertise-client-urls http://0.0.0.0:2379,http://0.0.0.0:4001
```
```
docker run -d --restart unless-stopped \
    -p 2379:2379 \
    -p 2380:2380 \
    --name etcd \
    quay.io/coreos/etcd:v3.2.0 \
    /usr/local/bin/etcd \
    --data-dir=/etcd-data \
    --listen-peer-urls http://0.0.0.0:2380 \
    --advertise-client-urls http://0.0.0.0:2379 \
    --listen-client-urls http://0.0.0.0:2379
```