
const DiscoveryService = require("./engine/DiscoveryService");

ds = new DiscoveryService({
    hosts: "http://10.24.70.21:2379",
    ttl: 1,
    maxRetries: 10,
    timeout: 3000,
});

ds.test();