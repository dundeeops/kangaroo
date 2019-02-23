const net = require("net");
const es = require("event-stream");
const {
    getServerName,
    serializeData,
} = require("./SerializationUtil.js");
const {
    getPromise,
} = require("./PromisifyUtil");
const RestartService = require("./RestartService.js");

const DEFAULT_TIMEOUT_ERROR_MESSAGE = "TIMEOUT: Error starting a server"

const defaultOptions = {
    inject: {
        _net: net,
    },
};

module.exports = class WorkerServer {
    constructor(_options) {
        const options = {
            ...defaultOptions,
            ..._options,
        };
        this._hostname = options.hostname;
        this._port = options.port;
        this._getMappers = options.getMappers;
        this._onAsk = options.onAsk;
        this._onData = options.onData;

        this.initInjections(options);

        this.init(options);

        this.initPromise();
        this.initService(options);
    }

    initInjections(options) {
        this._net = options.inject._net;
    }

    init() {
        this._server = null;
    }

    initPromise(_getPromise = getPromise) {
        [this._promise, this._resolve] = _getPromise();
    }

    initService(options, _RestartService = RestartService) {
        this._service = new _RestartService({
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

    // TODO: Log error
    onError(error) {
        console.error(`Worker failed ${this.getName()}`, error.message, error.stack);
    }

    getName(_getServerName = getServerName) {
        return _getServerName(this._hostname, this._port);
    }

    async start() {
        this._service.start();
        await this._promise;
    }

    makeServer(onSocketConected, onConnect, onClose) {
        const server = this._net.createServer(onSocketConected);
        server.on("close", () => onClose());
        server.listen(this._port, this._hostname, () => onConnect());
        return server;
    }

    run(callback, _serializeData = serializeData, _es = es) {
        let server;
        server = this.makeServer(
            (socket) => {
                socket.write(_serializeData({
                    type: "info",
                    mappers: this._getMappers(),
                }) + "\n");

                socket 
                    .pipe(_es.split())
                    .pipe(_es.parse())
                    .pipe(_es.map(async (obj, cb) => {
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
                // TODO: Remove and replace with event
                console.log(`Worker started with ${this.getName()}`);
                this._server = server;
                callback();
                this._resolve();
            },
            () => {
                // TODO: Remove and replace with event
                console.log(`Worker stopped with ${this.getName()}`);
                this._server = null;
                callback();
            },
        );

        return server;
    }
}
