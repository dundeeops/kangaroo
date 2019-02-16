const ConnectionSocket = require("./ConnectionSocket.js");
const {
    getServerName,
} = require("./SerializationUtil.js");
const TimeoutErrorTimer = require("./TimeoutErrorTimer.js");

const POOLING_TIMEOUT = 5000;

module.exports = class ConnectionService {
    constructor(options) {
        this._connectionsMap = {};
        this._resolvers = [];
        this._resolversMap = {};
        this._connections = [];
        this._poolingConnectionsMap = {};
        this._poolingInterval = null;
        this._poolingTimeout = options.poolingTimeout || POOLING_TIMEOUT;
        
        (options.connections || []).forEach(
            (connectionConfig) => this.addConnection(
                connectionConfig.hostname,
                connectionConfig.port,
            ),
        );
        
        (options.poolingConnections || []).forEach(
            (connectionConfig) => this.addPoolConnection(
                connectionConfig.hostname,
                connectionConfig.port,
            ),
        );
    }

    async startPoolingConnections(timeout) {
        if (!this._poolingInterval) {
            await this.stickOutPoolConnections();
            this._poolingInterval = setInterval(() => {
                this.stickOutPoolConnections();
            }, timeout || this._poolingTimeout);
        }
    }

    stopPoolingConnections() {
        if (this._poolingInterval) {
            clearInterval(this._poolingInterval);
            this._poolingInterval = null;
        }
    }

    async connectPoolingConnections() {
        const promises = [];
        for (const name in this._poolingConnectionsMap) {
            const { hostname, port } = this._poolingConnectionsMap[name];
            let resolve = () => {};
            const promise = new Promise((r) => resolve = r);
            promises.push(promise);
            let connection;
            connection = new ConnectionSocket({
                hostname, port,
                onReceiveInfo: (info) => {
                    resolve(connection);
                },
                onError: (error) => {
                    resolve();
                },
                restart: {
                    restart: false,
                    onErrorTimeout: () => {
                        resolve();
                        connection.destroy();
                    },
                },
            });
            connection.connect();
        }
        const connectionsRaw = await Promise.all(promises);
        return connectionsRaw.filter((connection) => !!connection);
    }

    async stickOutPoolConnections() {
        const connections = await this.connectPoolingConnections();
        connections.forEach((connection) => {
            const name = connection.getName();
            const hostname = connection.getHostname();
            connection.setRestart(true);
            connection.setOnErrorTimeout(() => {
                this.removeConnection(connection);
                this.addPoolConnection(hostname, port);
                connection.destroy();
            })
            const port = connection.getPort();
            this._connections.push(connection);
            this._connectionsMap[name] = connection;
            this.removePoolConnection(hostname, port);
        });
    }

    removePoolConnection(hostname, port) {
        const name = getServerName(hostname, port);
        delete this._poolingConnectionsMap[name];
    }

    addPoolConnection(hostname, port) {
        const name = getServerName(hostname, port);
        this._poolingConnectionsMap[name] = { hostname, port };
    }

    removeConnection(hostname, port) {
        const name = getServerName(hostname, port);
        this.removeItem(this._resolversMap, this._resolvers, name, this._resolversMap[name] && this._resolversMap[name].promise);
        this.removeItem(this._connectionsMap, this._connections, name);
    }

    removeItem(map, arr, hash, item) {
        const index = arr.indexOf(item || map[hash]);
        if (index > -1) {
            arr.splice(index, 1);
        }
        delete map[hash];
    }

    addConnection(hostname, port) {
        const name = getServerName(hostname, port);

        this._resolversMap[name] = {};
        const promise = new Promise((resolve) => {
            this._resolversMap[name].resolve = resolve;
        }).then(() => {
            this.removeItem(this._resolversMap, this._resolvers, name, promise);
        });
        this._resolversMap[name].promise = promise;

        this._resolvers.push(promise);

        let connection;
        connection = new ConnectionSocket({
            hostname, port,
            onReceiveInfo: (info) => {
                this._resolversMap[name] && this._resolversMap[name].resolve();
            },
            restart: {
                onErrorTimeout: () => {
                    resolve();
                    connection.destroy();
                },
            },
        });

        this._connectionsMap[name] = connection;
        this._connections.push(connection);

        return [connection, promise];
    }

    async run() {
        const timeout = new TimeoutErrorTimer();
        timeout.start("TIMEOUT: Error connecting with workers");
        this._connections.forEach((connection) => {
            connection.connect();
        });
        await Promise.all(this._resolvers);
        await this.startPoolingConnections();
        timeout.stop();
    }

    getConnections() {
        return this._connections;
    }

    getConnection(name) {
        return this._connectionsMap[name];
    }
}
