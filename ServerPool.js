const ServerConnection = require("./ServerConnection.js");

module.exports = class ServerPool {
    constructor(options) {
        this._serversMap = {};
        this._servers = options.servers
            .map(
                (serverConfig) => {
                    const server = new ServerConnection({
                        name: serverConfig.name,
                        ...serverConfig,
                    });
                    this._serversMap[serverConfig.name] = server;
                    return server;
                },
            );
    }

    getServers() {
        return this._servers;
    }

    getServer(name) {
        return this._serversMap[name];
    }
}
