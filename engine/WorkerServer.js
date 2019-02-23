const net = require("net");
const es = require("event-stream");
const {
    getServerName,
    serializeData,
} = require("./SerializationUtil.js");
const RestartService = require("./RestartService.js");

const DEFAULT_TIMEOUT_ERROR_MESSAGE = "TIMEOUT: Error starting a server"

module.exports = class WorkerServer {
    constructor(options) {
        this._hostname = options.hostname;
        this._port = options.port;
        this._getMappers = options.getMappers;
        this._onAsk = options.onAsk;
        this._onData = options.onData;

        this._server = null;

        this._resolve = () => {};
        this._promise = new Promise((r) => this._resolve = r);

        this._service = new RestartService({
            onError: this.onError.bind(this),
            onErrorTimeout: (error) => {
                throw error;
            },
            run: this.run.bind(this),
            isAlive: () => !!this._server,
            timeoutErrorMessage: DEFAULT_TIMEOUT_ERROR_MESSAGE,
            ...options.restart,
        });
    }

    onError(error) {
        console.error(`Worker failed ${this.getName()}`, error.message, error.stack);
    }

    getName() {
        return getServerName(this._hostname, this._port);
    }

    async start() {
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
                    .pipe(es.split())
                    .pipe(es.parse())
                    .pipe(es.map(async (obj, cb) => {
                        const { id, type, data } = obj;
                        if (type) {
                            await this._onAsk(socket, id, type, data);
                        } else {
                            await this._onData(socket, obj);
                        }
                        cb();
                    }));
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
}
