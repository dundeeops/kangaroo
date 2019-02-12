const { Readable } = require("stream");
const net = require("net");
const domain = require("domain");

const RESTART_TIMEOUT = 50000;

module.exports = class ServerEntry {
    constructor(options) {
        this._name = options.name;
        this._hostname = options.hostname;
        this._port = options.port;
        this._stream = this.makeStream();
        this._server = null;
        this._isStarting = false;
        this._restartInterval = null;
        this._shouldRestart = options.restart != null ? options.restart : true;
        this._restartTimeout = options.restartTimeout || RESTART_TIMEOUT;
        this._domain = domain.create();
        this._domain.on("error", this.onError.bind(this));
    }

    onError(error) {
        console.error(`Worker failed ${this._name}`, error.message, error.stack);
        if (!this._server) {
            this._isStarting = false;
            this.startRestart();
        }
    }

    getName() {
        return this._name;
    }

    startRestart() {
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

    checkRunning() {
        if (!this._isStarting && !this._server) {
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

            const server = this.makeServer((socket) => {
                socket.on("data", (data) => {
                    this._stream.push(data.toString("utf8"));
                });
            }, () => {
                console.log(`Worker started with ${this._name}`);
                this.stopRestart();
                this._server = server;
                this._isStarting = false;
            }, () => {
                console.log(`Worker stopped with ${this._name}`);
                this._server = null;
                this._isStarting = false;
                this.startRestart();
            });

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
