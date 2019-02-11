const ServerConnection = require("./ServerConnection.js");

module.exports = class ServerPool {
    constructor(options) {
        this.servers = Object.keys(options.config.servers)
            .map(
                (name) => new ServerConnection({
                    name,
                    serverConfig: options.config.servers[name],
                }),
            );
    }

    getServer(name) {
        return config.servers[name];
    }
}
