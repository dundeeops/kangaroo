# Simple test

> [../examples/simple/index.js](../examples/simple/index.js)

## Running

`node index.js`

# Additional Testing Commands

# Running etcd:

```
docker run -d --restart unless-stopped \
    -p 2379:2379 \
    -p 4001:4001 \
    --name etcd \
    127.0.0.1:5000/elcolio/etcd:latest \
    -listen-client-urls http://0.0.0.0:2379,http://0.0.0.0:4001 -advertise-client-urls http://0.0.0.0:2379,http://0.0.0.0:4001
```