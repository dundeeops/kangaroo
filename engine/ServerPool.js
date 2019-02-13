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
        this._poolServersMap = {};
        
        options.servers.forEach(
            (serverConfig) => this.addServer(
                serverConfig.hostname,
                serverConfig.port,
            ),
        );
    }

    async connectServerPool() {
        const promises = [];
        for (const { hostname, port } of this._poolServersMap) {
            const resolve = () => {};
            const promise = new Promise((r) => resolve = r);
            promises.push(promise);
            let server;
            server = new ServerConnection({
                hostname, port,
                reconnect: false,
                onReceiveInfo: (info) => {
                    resolve(server);
                },
                onError: () => {
                    r();
                },
                onErrorTimeout: () => {
                    r();
                    server.destroy();
                }
            });
            server.checkConnection();
        }
        const serversRaw = await Promise.all(this._resolvers);
        servers = serversRaw.filter((server) => !!server);
        return servers;
    }

    async stickOutPool() {
        const servers = await this.connectServerPool();
        servers.forEach((server) => {
            const name = server.getName();
            const hostname = server.getHostname();
            server.setReconnect(true);
            server.setOnErrorTimeout(() => {
                this.removeServer(server);
                this.addPoolServer(hostname, port);
                server.destroy();
            })
            const port = server.getPort();
            this._servers.push(server);
            this._serversMap[name] = server;
            this.removePoolServer(hostname, port);
        });
    }

    moveServerToPool(hostname, port) {
        this.removeServer(hostname, port);
        this.addPoolServer(hostname, port);
    }

    movePoolToServer(hostname, port) {
        this.removePoolServer(hostname, port);
        this.addServer(hostname, port);
    }

    removePoolServer(hostname, port) {
        const name = getServerName(hostname, port);
        delete this._poolServersMap[name];
    }

    addPoolServer(hostname, port) {
        const name = getServerName(hostname, port);
        this._poolServersMap[name] = { hostname, port };
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

        let server;
        server = new ServerConnection({
            hostname, port,
            onReceiveInfo: (info) => {
                this._resolversMap[name] && this._resolversMap[name].resolve();
            },
            onErrorTimeout: () => {
                this.removeServer(server);
                this.addPoolServer(hostname, port);
                server.destroy();
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
