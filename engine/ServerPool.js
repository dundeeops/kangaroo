const ServerConnection = require("./ServerConnection.js");
const {
    getServerName,
} = require("./Serialization.js");
const TimeoutError = require("./TimeoutError.js");

module.exports = class ServerPool {
    constructor(options) {
        this._serversMap = {};
        this._resolvers = [];
        this._resolversMap = {};
        this._servers = [];
        
        options.servers.forEach(
            (serverConfig) => this.addServer(
                serverConfig.hostname,
                serverConfig.port,
            ),
        );
    }

    removeServer(hostname, port) {
        const name = getServerName(hostname, port);
        this.removeItem(this._resolversMap, this._resolvers, name, this._resolversMap[name] && this._resolversMap[name].promise);
        this.removeItem(this._serversMap, this._servers, name);
    }

    removeItem(map, arr, hash, item) {
        const index = arr.indexOf(item || map[hash]);
        if (index > -1) {
            arr.splice(index, 1);
        }
        delete map[hash];
    }

    addServer(hostname, port) {
        const name = getServerName(hostname, port);

        this._resolversMap[name] = {};
        const promise = new Promise((resolve, e) => {
            this._resolversMap[name].resolve = resolve;
        }).then(() => {
            this.removeItem(this._resolversMap, this._resolvers, name, promise);
        });
        this._resolversMap[name].promise = promise;

        this._resolvers.push(promise);

        const server = new ServerConnection({
            hostname, port,
            onReceiveInfo: (info) => {
                console.log(info);
                this._resolversMap[name] && this._resolversMap[name].resolve();
            }
        });

        this._serversMap[name] = server;
        this._servers.push(server);

        return [server, promise];
    }

    async run() {
        const timeout = new TimeoutError();
        timeout.start("TIMEOUT: Error connecting with workers");
        this._servers.forEach((server) => {
            server.checkConnection();
        });
        await Promise.all(this._resolvers);
        timeout.stop();
    }

    getServers() {
        return this._servers;
    }

    getServer(name) {
        return this._serversMap[name];
    }
}
