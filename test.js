
const DiscoveryService = require("./engine/DiscoveryService");

ds = new DiscoveryService({
    hosts: "http://127.0.0.1:2379",
});

ds.test();