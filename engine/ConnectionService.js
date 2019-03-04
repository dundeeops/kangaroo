const ConnectionSocket = require("./ConnectionSocket.js");
const {
    getServerName,
} = require("./SerializationUtil.js");
const {
    getPromise,
    raceData,
} = require("./PromiseUtil");
const TimeoutErrorTimer = require("./TimeoutErrorTimer.js");

const POOLING_TIMEOUT = 5000;
const TIMEOUT_ERROR_CONNECTION = "TIMEOUT: Error connecting with workers";

const defaultOptions = {
    connections: [],
    poolingConnections: [],
    poolingTimeout: POOLING_TIMEOUT,
    onConnect: () => {},
    onConnectError: () => {},
    onConnectTimeoutError: () => {},
    inject: {
        _setInterval: setInterval,
        _clearInterval: clearInterval,
        _ConnectionSocket: ConnectionSocket,
        _TimeoutErrorTimer: TimeoutErrorTimer,
    },
};

module.exports = class ConnectionService {
    constructor(_options = {}) {
        const options = {
            ...defaultOptions,
            ..._options,
            inject: {
                ...defaultOptions.inject,
                ..._options.inject,
            },
        };
        this._poolingTimeout = options.poolingTimeout;
        this._onConnect = options.onConnect;
        this._onConnectError = options.onConnectError;
        this._onConnectTimeoutError = options.onConnectTimeoutError;

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
        options.connections.forEach(
            (connectionConfig) => this.addConnection(
                connectionConfig.hostname,
                connectionConfig.port,
            ),
        );
        
        options.poolingConnections.forEach(
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

                onReceiveInfo: (info) => {
                    resolve(connection);
                    this._onConnect(connection, info);
                },

                onError: (error) => {
                    resolve();
                    this._onConnectError(error, connection);
                },

                restart: {
                    restart: false,
                    onErrorTimeout: (error) => {
                        resolve();
                        connection.destroy();
                        this._onConnectTimeoutError(error, connection);
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
                this._onConnectTimeoutError(error, connection);
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

            onReceiveInfo: (info) => {
                resolve();
                this._onConnect(connection, info);
            },

            onError: (error) => {
                this._onConnectError(error, connection);
            },

            restart: {
                onErrorTimeout: () => {
                    resolve();
                    connection.destroy();
                    this._onConnectTimeoutError(error, connection);
                },
            },
        });

        this._connectionsMap.set(name, connection);

        return [connection, promise];
    }

    async addConnectionAndConnect(hostname, port) {
        const [connection, promise] = this.addConnection(hostname, port);
        connection.connect();
        await promise;
        return connection;
    }

    async start() {
        const timeout = new this._TimeoutErrorTimer();
        timeout.start(TIMEOUT_ERROR_CONNECTION);

        this._connectionsMap.forEach((connection) => {
            connection.connect();
        });

        await Promise.all(
            Array.from(
                this._resolversMap.values(),
            )
            .map((({promise}) => promise)),
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

    uploadModuleStream(readStream, data) {
        this.reduceConnections(
            (connection) => readStream.pipe(connection.getUploadModuleStream(data)),
        );
        return readStream;
    }

    reduceConnections(callback) {
        const array = [];
        this._connectionsMap
            .forEach((connection) => {
                array.push(callback(connection));
            });
        return array;
    }

    async notify(type, data) {
        const promises = this.reduceConnections(
            (connection) => connection.notify(type, data),
        );
        await Promise.all(promises);
    }

    async findConnection(type, data, _raceData = raceData) {
        const promises = this.reduceConnections(
            (connection) => connection.findConnection(type, data),
        );
        return await _raceData(promises);
    }

    askPromises(type, data) {
        const promises = this.reduceConnections(
            (connection) => connection.ask(type, data),
        );
        return promises;
    }

    async ask(type, data, _raceData = raceData) {
        const promises = this.askPromises(type, data);
        return await _raceData(promises);
    }

    async askAll(type, data) {
        const promises = this.askPromises(type, data);
        const result = await Promise.all(promises);
        return result.filter((r) => !!r);
    }
}
