const ConnectionSocket = require("./ConnectionSocket.js");
const {
    getServerName,
} = require("./SerializationUtil.js");
const TimeoutErrorTimer = require("./TimeoutErrorTimer.js");

const POOLING_TIMEOUT = 5000;

module.exports = class ConnectionService {
    constructor(options) {
        this._serversMap = {};
        this._resolvers = [];
        this._resolversMap = {};
        this._servers = [];
        this._poolingServersMap = {};
        this._poolingInterval = null;
        this._poolingTimeout = options.poolingTimeout || POOLING_TIMEOUT;
        
        (options.servers || []).forEach(
            (serverConfig) => this.addServer(
                serverConfig.hostname,
                serverConfig.port,
            ),
        );
        
        (options.poolingServers || []).forEach(
            (serverConfig) => this.addPoolServer(
                serverConfig.hostname,
                serverConfig.port,
            ),
        );
    }

    async startPoolingServers(timeout) {
        if (!this._poolingInterval) {
            await this.stickOutPoolServers();
            this._poolingInterval = setInterval(() => {
                this.stickOutPoolServers();
            }, timeout || this._poolingTimeout);
        }
    }

    stopPoolingServers() {
        if (this._poolingInterval) {
            clearInterval(this._poolingInterval);
            this._poolingInterval = null;
        }
    }

    async connectPoolingServers() {
        const promises = [];
        for (const name in this._poolingServersMap) {
            const { hostname, port } = this._poolingServersMap[name];
            let resolve = () => {};
            const promise = new Promise((r) => resolve = r);
            promises.push(promise);
            let server;
            server = new ConnectionSocket({
                hostname, port,
                reconnect: false,
                onReceiveInfo: (info) => {
                    resolve(server);
                },
                onError: (error) => {
                    resolve();
                },
                onErrorTimeout: () => {
                    resolve();
                    server.destroy();
                }
            });
            server.checkConnection();
        }
        const serversRaw = await Promise.all(promises);
        return serversRaw.filter((server) => !!server);
    }

    async stickOutPoolServers() {
        const servers = await this.connectPoolingServers();
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

    removePoolServer(hostname, port) {
        const name = getServerName(hostname, port);
        delete this._poolingServersMap[name];
    }

    addPoolServer(hostname, port) {
        const name = getServerName(hostname, port);
        this._poolingServersMap[name] = { hostname, port };
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
        server = new ConnectionSocket({
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
        const timeout = new TimeoutErrorTimer();
        timeout.start("TIMEOUT: Error connecting with workers");
        this._servers.forEach((server) => {
            server.checkConnection();
        });
        await Promise.all(this._resolvers);
        await this.startPoolingServers();
        timeout.stop();
    }

    getServers() {
        return this._servers;
    }

    getServer(name) {
        return this._serversMap[name];
    }
}
