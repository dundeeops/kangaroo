const ConnectionSocket = require("./ConnectionSocket.js");
const {
    getServerName,
} = require("./SerializationUtil.js");
const {
    getPromise,
} = require("./PromisifyUtil");
const TimeoutErrorTimer = require("./TimeoutErrorTimer.js");

const POOLING_TIMEOUT = 5000;
const TIMEOUT_ERROR_CONNECTION = "TIMEOUT: Error connecting with workers";

const defaultOptions = {
    poolingTimeout: POOLING_TIMEOUT,
    inject: {
        _setInterval: setInterval,
        _clearInterval: clearInterval,
        _ConnectionSocket: ConnectionSocket,
        _TimeoutErrorTimer: TimeoutErrorTimer,
    },
};

module.exports = class ConnectionService {
    constructor(_options) {
        const options = {
            ...defaultOptions,
            ..._options,
        };
        this._poolingTimeout = options.poolingTimeout;

        this.initInjections(options);

        this.init(options);
        this.initConnections(options);
    }

    initInjections(options) {
        this._setInterval = options.inject._setInterval;
        this._clearInterval = options.inject._clearInterval;
        this._ConnectionSocket = options.inject._ConnectionSocket;
        this._TimeoutErrorTimer = options.inject._TimeoutErrorTimer;
    }

    init() {
        this._poolingInterval = null;
        this._connectionsMap = new Map();
        this._resolversMap = new Map();
        this._poolingConnectionsMap = new Map();
    }

    initConnections(options) {
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

    async startPoolingConnections(timeout = this._poolingTimeout) {
        if (!this._poolingInterval) {
            await this.stickOutPoolConnections();
            this._poolingInterval = this._setInterval(() => {
                this.stickOutPoolConnections();
            }, timeout);
        }
    }

    stopPoolingConnections() {
        if (this._poolingInterval) {
            this._clearInterval(this._poolingInterval);
            this._poolingInterval = null;
        }
    }

    async connectPoolingConnections(_getPromise = getPromise) {
        const promises = [];

        this._poolingConnectionsMap.forEach(({ hostname, port }) => {

            const [promise, resolve] = _getPromise();
            promises.push(promise);

            const connection = new this._ConnectionSocket({
                hostname, port,

                // TODO: Store connection info
                onReceiveInfo: (info) => {
                    resolve(connection);
                },

                // TODO: Log connection error
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
        });

        const connectionsRaw = await Promise.all(promises);
        return connectionsRaw.filter((connection) => !!connection);
    }

    async stickOutPoolConnections() {
        const connections = await this.connectPoolingConnections();

        connections.forEach((connection) => {
            const name = connection.getName();
            const hostname = connection.getHostname();
            const port = connection.getPort();

            connection.setRestart(true);
            connection.setOnErrorTimeout(() => {
                this.removeConnection(connection);
                this.addPoolConnection(hostname, port);
                connection.destroy();
            });

            this.removePoolConnection(hostname, port);
            this._connectionsMap.set(name, connection);
        });
    }

    removePoolConnection(hostname, port, _getServerName = getServerName) {
        const name = _getServerName(hostname, port);
        this._poolingConnectionsMap.delete(name);
    }

    addPoolConnection(hostname, port, _getServerName = getServerName) {
        const name = _getServerName(hostname, port);
        this._poolingConnectionsMap.set(name, { hostname, port });
    }

    removeConnection(hostname, port, _getServerName = getServerName) {
        const name = _getServerName(hostname, port);

        this._resolversMap.delete(name);
        this._connectionsMap.delete(name);
    }

    addConnection(hostname, port, _getServerName = getServerName, _getPromise = getPromise) {
        const name = _getServerName(hostname, port);
        
        const [promise, resolve] = _getPromise();
        this._resolversMap.set(name, { promise, resolve, });
        promise.then(() => {
            this._resolversMap.delete(name);
        });

        const connection = new this._ConnectionSocket({
            hostname, port,

            // TODO: Store connection info
            onReceiveInfo: (info) => {
                resolve();
            },

            // TODO: Log connection error
            onError: (error) => {},

            restart: {
                onErrorTimeout: () => {
                    resolve();
                    connection.destroy();
                },
            },
        });

        this._connectionsMap.set(name, connection);

        return [connection, promise];
    }

    async start() {
        const timeout = new this._TimeoutErrorTimer();
        timeout.start(TIMEOUT_ERROR_CONNECTION);

        this._connectionsMap.forEach((connection) => {
            connection.connect();
        });

        await Promise.all(
            Array.from(
                this._resolversMap.values()
            )
        );

        await this.startPoolingConnections();

        timeout.stop();
    }

    getConnections() {
        return this._connectionsMap;
    }

    getConnection(name) {
        return this._connectionsMap.get(name);
    }
}
