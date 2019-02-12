const { Writable } = require("stream");
const net = require("net");

const RECONNECT_TIMEOUT = 100;

module.exports = class ServerConnection {
    constructor(options) {
        this.name = options.name;
        this.hostname = options.hostname;
        this.port = options.port;
        this._stream = this.makeStream();
        this._isConnecting = false;
        this._accumulation = [];
        this._reconnectionInterval = null;
        this._shouldReconnect = options.reconnect != null ? options.reconnect : true;
        this._reconnectTimeout = options.reconnectTimeout || RECONNECT_TIMEOUT;
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
        }
    }

    async connect() {
        this._isConnecting = true;
        let socket;
        await new Promise(async (r) => {
            socket = new net.Socket();

            socket.on("data", (data) => {
                this._stream.push(data.toString("utf8"));
            });

            socket.on("close", () => {
                console.log(`Disconnected with ${this.name}`);
                this._socket = null;
                r();
                this.startReconnection();
            });

            socket.on("connect", (data) => {
                console.log(`Connected with ${this.name}`);
                r();
            });

            socket.connect(
                this.port,
                this.hostname,
            );
        });
        this._socket = socket;
        this._isConnecting = false;
        this.releaseAccumulator();
    }

    checkConnection() {
        if (!this._isConnecting && !this._socket) {
            this.connect();
        }
    }

    makeStream() {
        return new Writable({
            write(chunk, encoding, callback) {
                callback();
            },
            final(callback) {
                callback();
            }
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
