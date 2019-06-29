const net = require("net");
const EventEmitter = require("events");
const es = require("event-stream");
const {
    getPromise,
} = require("./PromiseUtil.js");
const RestartService = require("./RestartService.js");

const TIMEOUT_ERROR_MESSAGE = "TIMEOUT: Error starting a server"

const defaultOptions = {
    onConnect: () => {},
    onData: () => {},
    onError: () => {},
    onErrorTimeout: () => {},
    inject: {
        _net: net,
        _es: es,
        _RestartService: RestartService,
    },
};

module.exports = class WorkerServer extends EventEmitter {
    constructor(_options = {}) {
        super();
        const options = {
            ...defaultOptions,
            ..._options,
            inject: {
                ...defaultOptions.inject,
                ..._options.inject,
            },
        };
        this._hostname = options.hostname;
        this._port = options.port;
        this._onConnect = options.onConnect;
        this._onData = options.onData;
        this._onError = options.onError;
        this._onErrorTimeout = options.onErrorTimeout;

        this.initInjections(options);
        this.init(options);
        this.initPromise();
        this.initService(options);
    }

    initInjections(options) {
        this._net = options.inject._net;
        this._es = options.inject._es;
        this._RestartService = options.inject._RestartService;
    }

    init() {
        this._server = null;
    }

    initPromise(_getPromise = getPromise) {
        [this._promise, this._resolve] = _getPromise();
    }

    initService(options) {
        this._service = new this._RestartService({
            onError: this.onError.bind(this),
            onErrorTimeout: (error) => {
                this._onErrorTimeout(error);
            },
            run: this.run.bind(this),
            isAlive: () => !!this._server,
            timeoutErrorMessage: TIMEOUT_ERROR_MESSAGE,
            ...options.restart,
        });
    }

    onError(error) {
        this._onError(error);
    }

    async start() {
        this._service.start();
        await this._promise;
    }

    makeServer(onSocketConnected, onConnect, onClose) {
        const server = this._net.createServer(onSocketConnected);
        server.on("close", () => onClose());
        server.on("error", (error) => this.onError(error));
        server.on("listening", () => onConnect());
        server.listen(this._port, this._hostname);
        return server;
    }

    onSocket(socket) {
        // socket.on('readable', async () => {
        //     try {
        //         let data;
        //         // read 100 bytes of data at a time
        //         while ((data = socket.read(100))) {
        //             await wait(1);
        //       }
        //     } catch (ex) {
        //         console.error(ex);
        //     }
        // });
        socket
            .pipe(this._es.split())
            .pipe(this._es.parse())
            .pipe(this._es.map(async (obj, callback) => {
                socket.pause();
                await this._onData(socket, obj);
                callback(null, obj);
                socket.resume();
            }));
    }

    run(callback) {
        const server = this.makeServer(
            async (socket) => {
                await this._onConnect(socket);
                this.onSocket(socket);
                this.emit("socket", socket, this);
            },
            () => {
                this._server = server;
                callback();
                this._resolve();
                this.emit("connect", this);
            },
            () => {
                this._server = null;
                callback();
                this.emit("close", this);
            },
        );
        return server;
    }
}
