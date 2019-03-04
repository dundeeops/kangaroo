const net = require("net");
const { Writable } = require("stream");
const {
    getServerName,
    serializeData,
    parseData,
    getId,
} = require("./SerializationUtil.js");
const {
    getPromise,
} = require("./PromiseUtil");
const RestartService = require("./RestartService.js");
const TimeoutErrorTimer = require("./TimeoutErrorTimer.js");
const AskDict = require("./AskDict.js");
const BaseDict = require("./BaseDict.js");

const DEFAULT_ASK_TIMEOUT = 1000;
const TIMEOUT_ERROR_MESSAGE = "TIMEOUT: Error connecting with a server $0:$1";

const defaultOptions = {
    onReceiveInfo: () => {},
    onError: () => {},
    onConnect: () => {},
    onDisconnect: () => {},
    askTimeout: DEFAULT_ASK_TIMEOUT,
    inject: {
        _net: net,
        _TimeoutErrorTimer: TimeoutErrorTimer,
        _RestartService: RestartService,
    },
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
        this._onReceiveInfo = options.onReceiveInfo;
        this._onError = options.onError;
        this._onConnect = options.onConnect;
        this._onDisconnect = options.onDisconnect;
        this._hostname = options.hostname;
        this._port = options.port;
        this._askTimeout = options.askTimeout;

        this.initInjections(options);
        this.init(options);
        this.initService(options);
    }

    initInjections(options) {
        this._net = options.inject._net;
        this._TimeoutErrorTimer = options.inject._TimeoutErrorTimer;
        this._RestartService = options.inject._RestartService;
    }

    init() {
        this._socket = null;
        this._accumulatedData = [];
        this._promiseAskMap = new Map();
    }

    initService(options) {
        this._service = new this._RestartService({
            run: this.run.bind(this),
            isAlive: this.isAlive.bind(this),
            onError: this.onError.bind(this),
            timeoutErrorMessage: TIMEOUT_ERROR_MESSAGE
                .replace("$0", this._hostname)
                .replace("$1", this._port),
            ...options.restart,
        });
    }

    onAnswer({id, data}) {
        if (this._promiseAskMap.get(id)) {
            this._promiseAskMap.get(id).resolve(data);
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

    async ask(type, data, onError = () => {}, _getId = getId) {
        const hash = _getId();
        const ask = this.addAskPromise(hash);
        const timeoutError = new this._TimeoutErrorTimer({
            timeout: this._askTimeout,
            onError: () => {
                ask.resolve();
                onError();
            },
        });
        timeoutError.start();
        this.push(serializeData({
            id: hash, type, data,
        }) + BaseDict.ENDING);
        const result = await ask.promise;
        timeoutError.stop();
        this.deleteAskPromise(hash);
        return result;
    }

    async findConnection(type, data) {
        const result = await this.ask(type, data);
        return result ? this : null;
    }

    async notify(type, data) {
        this.push(serializeData({
            type, data,
        }) + BaseDict.ENDING);
    }

    getUploadModuleStream(_info, _getId = getId) {
        const connection = this;
        const id = _getId().replace("/", "_").replace("\\", "_");
        const type = AskDict.UPLOAD;
        let isInit = false;
        const stream = new Writable({
            async read() {},
            async write(bytes, encoding, callback) {
                const info = isInit ? null : _info;
                isInit = true;
                await connection.notify(type, { id, info, bytes });
                callback(null, bytes);
            },
            async final(callback) {
                await connection.notify(type, { id, info: null, bytes: null });
                callback(null);
            }
        });
        return stream;
    }

    isAlive() {
        return !!this._socket;
    }

    onError(error) {
        this._onError(error);
    }

    setRestart(value) {
        this._service.setRestart(value);
    }

    setOnErrorTimeout(value) {
        this._service.setOnErrorTimeout(value);
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

    run(callback, _parseData = parseData) {
        const socket = new this._net.Socket();

        // TODO: ES
        socket.on("data", (raw) => {
            raw.toString().split(BaseDict.ENDING).map((str) => {
                if (str) {
                    const data = _parseData(str);
                    if (data.type === AskDict.INFO) {
                        // TODO: Move outside
                        this._socket = socket;
                        callback();
        
                        this._onReceiveInfo();
                        this.releaseAccumulator();
                    } else if (data.type) {
                        this.onAnswer(data);
                    }
                }
            });
        });

        socket.on("close", () => {
            this._socket = null;
            callback();
            this._onDisconnect(socket);
        });

        socket.on("ready", () => {
            this._onConnect(socket);
        });

        socket.on("error", (error) => {
            this.onError(error);
        });

        socket.connect({
            host: this._hostname,
            port: this._port,
        });

        return socket;
    }

    destroy() {
        if (this._socket) {
            this._socket.destroy();
            this._socket = null;
        }
    }

    releaseAccumulator() {
        while (this._accumulatedData.length > 0 && !!this._socket) {
            if (this._socket.write(this._accumulatedData[0])) {
                this._accumulatedData.shift();
            }
        }
    }

    pushAccumulator(data) {
        this._accumulatedData.push(data);
        return true;
    }

    push(data) {
        this._service.start();

        if (!this._socket || this._accumulatedData.length > 0) {
            return this.pushAccumulator(data);
        } else {
            return this._socket.write(data);
        }
    }

    sendData(data) {
        return this.push(data);
    }
}
