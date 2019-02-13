const { Readable } = require("stream");
const net = require("net");
const domain = require("domain");
const {
    getServerName,
    serializeData,
} = require("./Serialization.js");
const TimeoutError = require("./TimeoutError.js");

const RESTART_TIMEOUT = 5000;

module.exports = class ServerEntry {
    constructor(options) {
        this._hostname = options.hostname;
        this._port = options.port;
        this._getMappers = options.getMappers;
        this._stream = this.makeStream();
        this._server = null;
        this._isStarting = false;
        this._restartInterval = null;
        this._shouldRestart = options.restart != null ? options.restart : true;
        this._restartTimeout = options.restartTimeout || RESTART_TIMEOUT;
        this._domain = domain.create();
        this._domain.on("error", this.onError.bind(this));
        this._timeoutError = new TimeoutError({
            message: "TIMEOUT: Error starting a server"
        });
        this._resolve = () => {};
        this._promise = new Promise((r) => this._resolve = r);
    }

    onError(error) {
        console.error(`Worker failed ${this.getName()}`, error.message, error.stack);
        if (!this._server) {
            this._isStarting = false;
            this.startRestart();
        }
    }

    getName() {
        return getServerName(this._hostname, this._port);
    }

    startRestart() {
        this._timeoutError.start();
        this.stopRestart();
        this._restartInterval = setInterval(() => {
            if (this._shouldRestart) {
                this.checkRunning();
            }
        }, this._restartTimeout);
    }

    stopRestart() {
        if (this._restartInterval) {
            clearInterval(this._restartInterval);
            this._restartInterval = null;
        }
    }

    async runAndWait() {
        this.checkRunning();
        await this._promise;
    }

    checkRunning() {
        if (!this._isStarting && !this._server) {
            this._timeoutError.start();
            this.run();
        }
    }

    makeServer(onSocketConected, onConnect, onClose) {
        const server = net.createServer(onSocketConected);
        server.on("close", () => onClose());
        server.listen(this._port, this._hostname, () => onConnect());
        return server;
    }

    run() {
        this._domain.run(() => {
            this._isStarting = true;

            const server = this.makeServer(
                (socket) => {
                    socket.write(serializeData({
                        type: "info",
                        mappers: this._getMappers(),
                    }) + "\n");
                    socket.on("data", (data) => {
                        this._stream.push(data.toString("utf8"));
                    });
                },
                () => {
                    console.log(`Worker started with ${this.getName()}`);
                    this.stopRestart();
                    this._server = server;
                    this._isStarting = false;
                    this._resolve();
                    this._timeoutError.stop();
                },
                () => {
                    console.log(`Worker stopped with ${this.getName()}`);
                    this._server = null;
                    this._isStarting = false;
                    this.startRestart();
                },
            );

            return server;
        });
    }

    makeStream() {
        return new Readable({
            autoDestroy: true,
            read() {},
        });
    }

    getStream() {
        return this._stream;
    }
}
