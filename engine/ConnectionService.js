const ConnectionSocket = require("./ConnectionSocket.js");
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
                connectionConfig,
            ),
        );
        
        options.poolingConnections.forEach(
            (connectionConfig) => this.addPoolConnection(
                connectionConfig,
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

    connectionFactory(connectionConfig, callback) {
        const connection = new this._ConnectionSocket({
            key: connectionConfig,
            ...connectionConfig,
            onConnect: () => {
                callback(connection);
                this._onConnect(connection);
            },
            onError: (error) => {
                this._onConnectError(error, connection);
            },
            restart: {
                restart: false,
                onErrorTimeout: (error) => {
                    callback();
                    connection.close();
                    this._onConnectTimeoutError(error, connection);
                },
            },
        });
        return connection;
    }

    async connectPoolingConnections(_getPromise = getPromise) {
        const promises = [];
        this._poolingConnectionsMap.forEach((connectionConfig) => {
            const [promise, resolve] = _getPromise();
            promises.push(promise);
            const connection = this.connectionFactory(connectionConfig, resolve);
            connection.connect();
        });
        const connectionsRaw = await Promise.all(promises);
        return connectionsRaw.filter((connection) => !!connection);
    }
    
    setConnectionHandlers(connection) {
        connection.setRestart(true);
        connection.setOnErrorTimeout((error) => {
            this.removeConnection(connection);
            this.addPoolConnection(connection.key);
            connection.close();
            this._onConnectTimeoutError(error, connection);
        });
    }

    async stickOutPoolConnections() {
        const connections = await this.connectPoolingConnections();
        connections.forEach((connection) => {
            this.setConnectionHandlers(connection);
            this.removePoolConnection(connection.key);
            this._connectionsMap.set(connection.key, connection);
        });
    }

    removePoolConnection(connectionConfig) {
        this._poolingConnectionsMap.delete(connectionConfig);
    }

    addPoolConnection(connectionConfig) {
        this._poolingConnectionsMap.set(connectionConfig, connectionConfig);
    }

    removeConnection(connectionConfig) {
        this._resolversMap.delete(connectionConfig);
        this._connectionsMap.delete(connectionConfig);
    }

    addConnection(connectionConfig, _getPromise = getPromise) {
        const [promise, resolve] = _getPromise();
        this._resolversMap.set(connectionConfig, { promise, resolve, });
        promise.then(() => {
            this._resolversMap.delete(connectionConfig);
        });
        const connection = this.connectionFactory(connectionConfig, resolve);
        this.setConnectionHandlers(connection);
        this._connectionsMap.set(connectionConfig, connection);
        return [connection, promise];
    }

    async addConnectionAndConnect(connectionConfig) {
        const [connection, promise] = this.addConnection(connectionConfig);
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

    getConnection(connectionKey) {
        return this._connectionsMap.get(connectionKey);
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

    async askLimit(type, data, limit = 20, timeout = 1000) {
        const promises = this.askPromises(type, data);
        const result = await Promise.all(promises);
        return result.filter((r) => !!r);
    }
}
