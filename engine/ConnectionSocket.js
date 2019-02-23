const { Writable } = require("stream");
const net = require("net");
const {
    getServerName,
    serializeData,
    deserializeData,
    getId,
} = require("./SerializationUtil.js");
const {
    getPromise,
} = require("./PromisifyUtil");
const RestartService = require("./RestartService.js");
const TimeoutError = require("./TimeoutErrorTimer.js");

const DEFAULT_ASK_TIMEOUT = 1000;
const ENDING = "\n";

const defaultOptions = {
    onReceiveInfo: () => {},
    onError: () => {},
    askTimeout: DEFAULT_ASK_TIMEOUT,
};

module.exports = class ConnectionSocket {
    constructor(_options) {
        const options = {
            ...defaultOptions,
            ..._options,
        };
        this._onReceiveInfo = options.onReceiveInfo;
        this._onError = options.onError;
        this._hostname = options.hostname;
        this._port = options.port;
        this._askTimeout = options.askTimeout;

        this.init(options);
        this.initService(options);
    }

    init() {
        this._socket = null;
        this._info = {
            mappers: [],
        };
        this._accumulatedData = [];
        this._promiseAskMap = new Map();
    }

    initService(options, _RestartService = RestartService) {
        this._service = new _RestartService({
            onError: this.onError.bind(this),
            run: this.run.bind(this),
            isAlive: this.isAlive.bind(this),
            timeoutErrorMessage: `TIMEOUT: Error connecting with a server ${this._hostname}:${this._port}`,
            ...options.restart,
        });
    }

    onAsk({id, data}) {
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

    async ask(type, data, _getId = getId, _TimeoutError = TimeoutError) {
        const hash = _getId();
        const ask = this.addAskPromise(hash);

        const timeoutError = new _TimeoutError({
            timeout: this._askTimeout,
            // TODO: Log timeout error
            onError: () => ask.resolve(),
        });

        timeoutError.start();
        this.push(serializeData({
            id: hash, type, data,
        }) + ENDING);
        const result = await ask.promise;
        timeoutError.stop();

        this.deleteAskPromise(hash);

        return result;
    }

    async notify(type, data) {
        this.push(serializeData({
            type, data,
        }) + ENDING);
    }

    isAlive() {
        return !!this._socket;
    }

    onError(error) {
        // TODO: Remove
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

    async run(callback, _deserializeData = deserializeData) {
        const socket = new net.Socket();

        socket.on("data", (raw) => {
            raw.toString().split(ENDING).map((str) => {
                if (str) {
                    const data = _deserializeData(str);
                    if (data.type === "info") {
                        // TODO: Move outside
                        this._socket = socket;
                        this._info.mappers = data.mappers;
                        callback();
        
                        this._onReceiveInfo(this._info);
                        this.releaseAccumulator();
                    } else if (data.type) {
                        this.onAsk(data);
                    }
                }
            });
        });

        socket.on("close", () => {
            // TODO: Remove and replace with event
            console.log(`Disconnected with ${this.getName()}`);
            this._socket = null;
            callback();
        });

        socket.on("connect", () => {
            // TODO: Remove and replace with event
            console.log(`Connected with ${this.getName()}`);
        });

        socket.connect(
            this._port,
            this._hostname
        );

        return socket;
    }

    destroy() {
        if (this._socket) {
            this._socket.destroy();
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
