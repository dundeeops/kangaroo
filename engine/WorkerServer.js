const { Readable } = require("stream");
const net = require("net");
const EventEmitter = require("./EventEmitter.js");
const {
    getServerName,
    deserializeData,
    serializeData,
} = require("./SerializationUtil.js");
const RestartService = require("./RestartService.js");

module.exports = class WorkerServer extends EventEmitter {
    constructor(options) {
        super();
        this._hostname = options.hostname;
        this._port = options.port;
        this._getMappers = options.getMappers;
        this._onAnswer = options.onAnswer;

        this._server = null;
        this._stream = this.makeStream();
        
        this.on("answer", (socket, id, type, data) => {
            this._onAnswer(socket, id, type, data);
        });
        
        this.on("data", (data) => {
            this._stream.push(data);
        });

        this._resolve = () => {};
        this._promise = new Promise((r) => this._resolve = r);

        this._service = new RestartService({
            onError: this.onError.bind(this),
            onErrorTimeout: (error) => {
                throw error;
            },
            run: this.run.bind(this),
            isAlive: () => !!this._server,
            timeoutErrorMessage: "TIMEOUT: Error starting a server",
            ...options.restart,
        });
    }

    onError(error) {
        console.error(`Worker failed ${this.getName()}`, error.message, error.stack);
    }

    getName() {
        return getServerName(this._hostname, this._port);
    }

    async runAndWait() {
        this._service.start();
        await this._promise;
    }

    makeServer(onSocketConected, onConnect, onClose) {
        const server = net.createServer(onSocketConected);
        server.on("close", () => onClose());
        server.listen(this._port, this._hostname, () => onConnect());
        return server;
    }

    run(callback) {
        let server;
        server = this.makeServer(
            (socket) => {
                socket.write(serializeData({
                    type: "info",
                    mappers: this._getMappers(),
                }) + "\n");

                socket
                    .on("data", (raw) => {
                        raw.toString().split("\n").map((str) => {
                            if (str) {
                                const { id, type, data } = deserializeData(str);
        
                                if (type) {
                                    this.emit("answer", socket, id, type, data);
                                    // this._onAnswer(socket, id, type, data);
                                } else {
                                    this.emit("data", str);
                                    // this._stream.push(str);
                                }
                            }
                        });
                    });
                
                // socket.on("data", (raw) => {
                // });
            },
            () => {
                console.log(`Worker started with ${this.getName()}`);
                this._server = server;
                callback();
                this._resolve();
            },
            () => {
                console.log(`Worker stopped with ${this.getName()}`);
                this._server = null;
                callback();
            },
        );

        return server;
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
