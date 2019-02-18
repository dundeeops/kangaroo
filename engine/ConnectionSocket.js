const { Writable } = require("stream");
const net = require("net");
const EventEmitter = require("./EventEmitter.js");
const {
    getServerName,
    serializeData,
    deserializeData,
    getId,
    getHash,
} = require("./SerializationUtil.js");
const RestartService = require("./RestartService.js");
const TimeoutError = require("./TimeoutErrorTimer.js");

module.exports = class ConnectionSocket extends EventEmitter {
    constructor(options) {
        super();
        this._onReceiveInfo = options.onReceiveInfo || (() => {});
        this._onError = options.onError || (() => {});
        this._hostname = options.hostname;
        this._port = options.port;

        this._socket = null;
        this._stream = this.makeStream();
        this._info = {
            mappers: [],
        };
        this._accumulation = [];

        this._service = new RestartService({
            onError: this.onError.bind(this),
            run: this.run.bind(this),
            isAlive: this.isAlive.bind(this),
            timeoutErrorMessage: `TIMEOUT: Error connecting with a server ${this._hostname}:${this._port}`,
            ...options.restart,
        });
    }

    async ask(type, data) {
        return new Promise((r) => {
            const sessionId = getId();
            const name = getHash("ask", type);

            const timeoutError = new TimeoutError({
                timeout: 1000,
                onError: () => r(),
            });

            this.onceIfTrue(name, (id, type, data) => {
                if (id === sessionId) {
                    timeoutError.stop();
                    r(data);
                    return true;
                }
            });

            timeoutError.start();

            this.push(serializeData({
                id: sessionId, type, data,
            }) + "\n");
        });
    }

    isAlive() {
        return !!this._socket;
    }

    onError(error) {
        console.error(`Connection failed ${this.getName()}`, error.message, error.stack);
        this._onError(error);
    }

    setRestart(value) {
        this._service.setRestart(value);
    }

    setOnErrorTimeout(value) {
        this._service.setOnErrorTimeout(value);
    }

    isContainsStage(stage) {
        return this._info.mappers.includes(stage);
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

    connect() {
        this._service.start();
    }

    async run(callback) {
        const socket = new net.Socket();

        socket.on("data", (raw) => {
            raw.toString().split("\n").map((str) => {
                if (str) {
                    const data = deserializeData(str);
                    if (data.type === "info") {
                        this._socket = socket;
                        this._info.mappers = data.mappers;
                        callback();
        
                        this._onReceiveInfo(this._info);
                        this.releaseAccumulator();
                    } else if (data.type) {
                        const name = getHash("ask", data.type);
                        this.emit(name, data.id, data.type, data.data);
                    }
                }
            });
        });

        socket.on("close", () => {
            console.log(`Disconnected with ${this.getName()}`);
            this._socket = null;
            callback();
        });

        socket.on("connect", () => {
            console.log(`Connected with ${this.getName()}`);
        });

        socket.connect(
            this._port,
            this._hostname
        );

        return socket;
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
        this._service.start();

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
