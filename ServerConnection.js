const { Writable } = require("stream");
const net = require("net");
const domain = require("domain");

const RECONNECT_TIMEOUT = 500;

module.exports = class ServerConnection {
    constructor(options) {
        this._name = options.name;
        this._hostname = options.hostname;
        this._port = options.port;
        this._stream = this.makeStream();
        this._socket = null;
        this._isConnecting = false;
        this._accumulation = [];
        this._reconnectionInterval = null;
        this._shouldReconnect = options.reconnect != null ? options.reconnect : true;
        this._reconnectTimeout = options.reconnectTimeout || RECONNECT_TIMEOUT;
        this._domain = domain.create();
        this._domain.on("error", this.onError.bind(this));
    }

    onError(error) {
        console.error(`Connection failed ${this._name}`, error.message, error.stack);
        if (!this._socket) {
            this._isConnecting = false;
            this.startReconnection();
        }
    }

    getName() {
        return this._name;
    }

    startReconnection() {
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
            this.connect();
        }
    }

    connect() {
        this._domain.run(() => {
            this._isConnecting = true;

            const socket = new net.Socket();
    
            socket.on("data", (data) => {
                this._stream.push(data.toString("utf8"));
            });

            socket.on("close", () => {
                console.log(`Disconnected with ${this._name}`);
                this._socket = null;
                this._isConnecting = false;
                this.startReconnection();
            });

            socket.on("connect", (data) => {
                console.log(`Connected with ${this._name}`);
                this.stopReconnection();
                this._socket = socket;
                this.releaseAccumulator();
                this._isConnecting = false;
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
