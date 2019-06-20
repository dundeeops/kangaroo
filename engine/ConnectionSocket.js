const net = require("net");
const { Writable } = require("stream");
const es = require("event-stream");
const {
    makeMessage,
    parseData,
    getId,
} = require("./SerializationUtil.js");
const {
    getPromise,
} = require("./PromiseUtil");
const RestartService = require("./RestartService.js");
const TimeoutErrorTimer = require("./TimeoutErrorTimer.js");
const AskDict = require("./AskDict.js");

const DEFAULT_ASK_TIMEOUT = 5000;
const TIMEOUT_ERROR_MESSAGE = "TIMEOUT: Error connecting with a server $0:$1";

const defaultOptions = {
    managerServer: {
        hostname: "localhost",
        port: 2325,
    },
    dataServer: {
        hostname: "localhost",
        port: 2325,
    },
    onError: () => {},
    onConnect: () => {},
    onDisconnect: () => {},
    askTimeout: DEFAULT_ASK_TIMEOUT,
    inject: {
        _net: net,
        _TimeoutErrorTimer: TimeoutErrorTimer,
        _RestartService: RestartService,
    },
    restart: {},
};

module.exports = class ConnectionSocket {
    constructor(_options = {}) {
        const options = {
            ...defaultOptions,
            ..._options,
            inject: {
                ...defaultOptions.inject,
                ..._options.inject,
            },
        };
        this.key = options.key;
        this._onError = options.onError;
        this._onConnect = options.onConnect;
        this._onDisconnect = options.onDisconnect;
        this._managerServerOptions = options.managerServer;
        this._dataServerOptions = options.dataServer;
        this._askTimeout = options.askTimeout;

        this.initInjections(options);
        this.init(options);
        this.initServices(options);
    }

    initInjections(options) {
        this._net = options.inject._net;
        this._TimeoutErrorTimer = options.inject._TimeoutErrorTimer;
        this._RestartService = options.inject._RestartService;
    }

    init() {
        this._managerSocket = null;
        this._dataSocket = null;
        this._accumulatedData = [];
        this._promiseAskMap = new Map();
    }

    initServices(options) {
        this._managerService = new this._RestartService({
            run: this.runManagerSocket.bind(this),
            isAlive: this.isAliveManagerSocket.bind(this),
            onError: this.onError.bind(this),
            timeoutErrorMessage: TIMEOUT_ERROR_MESSAGE
                .replace("$0", this._managerServerOptions.hostname)
                .replace("$1", this._managerServerOptions.port),
            ...options.restart,
        });
        this._dataService = new this._RestartService({
            run: this.runDataSocket.bind(this),
            isAlive: this.isAliveDataSocket.bind(this),
            onError: this.onError.bind(this),
            timeoutErrorMessage: TIMEOUT_ERROR_MESSAGE
                .replace("$0", this._dataServerOptions.hostname)
                .replace("$1", this._dataServerOptions.port),
            ...options.restart,
        });
    }

    onAnswer({id, data}) {
        if (this._promiseAskMap.get(id)) {
            this._promiseAskMap.get(id).resolve(
                data && { data, connection: this },
            );
        }
    }

    makeAskPromise(promise, resolve) {
        return {
            promise, resolve,
        };
    }

    addAskPromise(hash, _getPromise = getPromise) {
        const [promise, resolve] = _getPromise();
        const ask = this.makeAskPromise(promise, resolve);
        this._promiseAskMap.set(hash, ask);
        return ask;
    }

    deleteAskPromise(hash) {
        this._promiseAskMap.delete(hash);
    }

    askTimeoutErrorFactory(onError) {
        return new this._TimeoutErrorTimer({
            timeout: this._askTimeout,
            onError: (error) => {
                onError(error);
            },
        });
    }

    async ask(type, data, onError = () => {}, _getId = getId) {
        const hash = _getId();
        const { promise, resolve } = this.addAskPromise(hash);
        const timeoutError = this.askTimeoutErrorFactory((error) => {
            resolve();
            onError(error);
        });
        timeoutError.start();
        this.pushToManager(
            makeMessage({
                id: hash, type, data, isAsk: true,
            }),
        );
        const answer = await promise;
        timeoutError.stop();
        this.deleteAskPromise(hash);
        return answer;
    }

    getInfoFactory(data) {
        let isInit = false;
        return () => {
            const info = isInit ? null : data;
            isInit = true;
            return info;
        };
    }

    getUploadModuleStream(data, _getId = getId) {
        const connection = this;
        const id = _getId();
        const type = AskDict.UPLOAD;
        const getInfo = this.getInfoFactory(data);
        const stream = new Writable({
            async write(bytes, encoding, callback) {
                const info = getInfo();
                await connection.notify(
                    type,
                    {
                        id, info, bytes,
                    },
                );
                callback();
            },
            async final(callback) {
                await connection.notify(
                    type,
                    {
                        id,
                    },
                );
                callback();
            }
        });
        return stream;
    }

    isAliveManagerSocket() {
        return !!this._managerSocket;
    }

    isAliveDataSocket() {
        return !!this._dataSocket;
    }

    onError(error) {
        this._onError(error);
    }

    setRestart(value) {
        this._managerService.setRestart(value);
        this._dataService.setRestart(value);
    }

    setOnErrorTimeout(value) {
        this._managerService.setOnErrorTimeout(value);
        this._dataService.setOnErrorTimeout(value);
    }

    connect() {
        this._managerService.start();
        this._dataService.start();
    }

    runManagerSocket(callback, _parseData = parseData) {
        const socket = new this._net.Socket();

        socket
            .pipe(es.split())
            .pipe(es.parse())
            .pipe(es.map((data, cb) => {
                this.onAnswer(data);
                cb(null, null);
            }));

        socket.on("close", () => {
            this._managerSocket = null;
            callback();
            this._onDisconnect(socket);
        });

        socket.on("ready", () => {
            this._managerSocket = socket;
            callback();
            this._onConnect(socket);
        });

        socket.on("error", (error) => {
            this.onError(error);
        });

        socket.connect({
            host: this._managerServerOptions.hostname,
            port: this._managerServerOptions.port,
        });

        return socket;
    }

    runDataSocket(callback, _parseData = parseData) {
        const socket = new this._net.Socket();

        socket.on("close", () => {
            this._dataSocket = null;
            callback();
            this._onDisconnect(socket);
        });

        socket.on("ready", () => {
            this._dataSocket = socket;
            callback();
            this._onConnect(socket);
            this.releaseAccumulator();
        });

        socket.on("error", (error) => {
            this.onError(error);
        });

        socket.connect({
            host: this._dataServerOptions.hostname,
            port: this._dataServerOptions.port,
        });

        return socket;
    }

    close() {
        if (this._managerSocket) {
            this._managerSocket.destroy();
            this._managerSocket = null;
        }

        if (this._dataSocket) {
            this._dataSocket.destroy();
            this._dataSocket = null;
        }
    }

    releaseAccumulator() {
        while (
            this._accumulatedData.length > 0
            && this.isAlive()
        ) {
            const message = this._accumulatedData[0];
            if (this._dataSocket.write(message)) {
                this._accumulatedData.shift();
            }
        }
    }

    push(message) {
        this._managerService.start();
        this._dataService.start();

        if (!this._dataSocket || this._accumulatedData.length > 0) {
            this._accumulatedData.push(message);
            return true;
        } else {
            return this._dataSocket.write(message);
        }
    }

    async notify(type, data) {
        this.pushToManager(
            makeMessage({
                type, data,
            }),
        );
    }

    pushToManager(message) {
        this._managerService.start();
        return this._managerSocket.write(message);
    }
}
