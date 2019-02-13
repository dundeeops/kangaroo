const ServerConnection = require("./ServerConnection.js");

module.exports = class ServerPool {
    constructor(options) {
        this._serversMap = {};
        this._resolvers = [];
        this._resolversMap = {};
        this._servers = options.servers
            .map(
                (serverConfig) => {
                    const resolve = new Promise((r, e) => {
                        this._resolversMap[serverConfig.name] = r;
                    })
                    this._resolvers.push(resolve);
                    const server = new ServerConnection({
                        name: serverConfig.name,
                        ...serverConfig,
                        onReceiveInfo: (info) => {
                            console.log(info);
                            
                            this._resolversMap[serverConfig.name]();
                        }
                    });
                    this._serversMap[serverConfig.name] = server;
                    return server;
                },
            );
    }

    async run() {
        this._servers.forEach((server) => {
            server.checkConnection();
        });
        await Promise.all(this._resolvers);
    }

    getServers() {
        return this._servers;
    }

    getServer(name) {
        return this._serversMap[name];
    }
}
