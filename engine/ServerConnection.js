const { Writable } = require("stream");
const net = require("net");
const domain = require("domain");
const {
    getServerName,
    deserializeData,
} = require("./Serialization.js");

const RECONNECT_TIMEOUT = 5000;
const CONNECT_TIMEOUT = 10000;

module.exports = class ServerConnection {
    constructor(options) {
        this._onReceiveInfo = options.onReceiveInfo || (() => {});
        this._onError = options.onError || (() => {});
        this._onErrorTimeout = options.onErrorTimeout || (() => {});
        this._hostname = options.hostname;
        this._port = options.port;
        this._stream = this.makeStream();
        this._socket = null;
        this._info = {
            mappers: [],
        };
        this._isAlive = false;
        this._isConnecting = false;
        this._accumulation = [];
        this._reconnectionInterval = null;
        this._shouldReconnect = options.reconnect != null ? options.reconnect : true;
        this._reconnectTimeout = options.reconnectTimeout || RECONNECT_TIMEOUT;
        this._connectTimeout = options.connectTimeout || CONNECT_TIMEOUT;
        this._domain = domain.create();
        this._domain.on("error", this.onError.bind(this));
        this._timeoutError = null;
    }

    startTimeout() {
        if (!this._timeoutError) {
            this._timeoutError = setTimeout(() => {
                this._onErrorTimeout();
            }, this._connectTimeout);
        }
    }

    stopTimeout() {
        if (this._timeoutError) {
            clearTimeout(this._timeoutError);
        }
        this._timeoutError = null;
    }

    setReconnect(value) {
        this._shouldReconnect = value;
    }

    setOnErrorTimeout(value) {
        this._onErrorTimeout = value;
    }

    isAlive() {
        return this._isAlive;
    }

    isContainsStage(stage) {
        return this._info.mappers.includes(stage);
    }

    onError(error) {
        console.error(`Connection failed ${this.getName()}`, error.message, error.stack);
        this._onError(error);
        if (!this._socket) {
            this._isConnecting = false;
            this._isAlive = false;
            this.startReconnection();
        }
    }

    getName() {
        return getServerName(this._hostname, this._port);
    }

    getHostname() {
        return this._hostname;
    }

    getPort() {
        return this._port;
    }

    startReconnection() {
        this.startTimeout();
        this.stopReconnection();
        this._reconnectionInterval = setInterval(() => {
            if (this._shouldReconnect) {
                this.checkConnection();
            }
        }, this._reconnectTimeout);
    }

    stopReconnection() {
        if (this._reconnectionInterval) {
            clearInterval(this._reconnectionInterval);
            this._reconnectionInterval = null;
        }
    }

    checkConnection() {
        if (!this._isConnecting && !this._socket) {
            this.startTimeout();
            this.connect();
        }
    }

    async connect() {
        this._domain.run(() => {
            this._isConnecting = true;

            const socket = new net.Socket();
    
            socket.on("data", (raw) => {
                const data = deserializeData(raw.toString());
                if (data.type === "info") {
                    this.stopTimeout();
                    this._info.mappers = data.mappers;
                    this._onReceiveInfo(this._info);
                } else if (!this._info) {
                    this.destroy();
                    throw new Error(`Connection failed, expected info ${this.getName()}`);
                }
            });

            socket.on("close", () => {
                console.log(`Disconnected with ${this.getName()}`);
                this._socket = null;
                this._isConnecting = false;
                this._isAlive = false;
                this.startReconnection();
            });

            socket.on("connect", (data) => {
                console.log(`Connected with ${this.getName()}`);
                this.stopReconnection();
                this._socket = socket;
                this.releaseAccumulator();
                this._isConnecting = false;
                this._isAlive = true;
            });

            socket.connect(
                this._port,
                this._hostname
            );

            return socket;
        });
    }

    makeStream() {
        return new Writable({
            autoDestroy: true,
            write(chunk, encoding, callback) {
                callback();
            },
        });
    }

    getStream() {
        return this._stream;
    }

    destroy() {
        if (this._socket) {
            this._socket.destroy();
        }
    }

    releaseAccumulator() {
        while (this._accumulation.length > 0 && !!this._socket) {
            if (this._socket.write(this._accumulation[0])) {
                this._accumulation.shift();
            }
        }
    }

    pushAccumulator(data) {
        this._accumulation.push(data);
        return true;
    }

    push(data) {
        this.checkConnection();

        if (!this._socket || this._accumulation.length > 0) {
            return this.pushAccumulator(data);
        } else {
            return this._socket.write(data);
        }
    }

    sendData(data) {
        this.push(data);
    }
}
