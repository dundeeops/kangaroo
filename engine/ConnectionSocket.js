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

module.exports = class ConnectionSocket {
    constructor(options) {
        this._onReceiveInfo = options.onReceiveInfo || (() => {});
        this._onError = options.onError || (() => {});
        this._hostname = options.hostname;
        this._port = options.port;
        this._askTimeout = options.askTimeout || DEFAULT_ASK_TIMEOUT;

        this._socket = null;
        this._stream = this.makeStream();
        this._info = {
            mappers: [],
        };
        this._accumulation = [];
        this._promiseAskMap = {};

        this._service = new RestartService({
            onError: this.onError.bind(this),
            run: this.run.bind(this),
            isAlive: this.isAlive.bind(this),
            timeoutErrorMessage: `TIMEOUT: Error connecting with a server ${this._hostname}:${this._port}`,
            ...options.restart,
        });
    }

    onAsk({id, data}) {
        if (this._promiseAskMap[id]) {
            this._promiseAskMap[id].resolve(data);
        }
    }

    makeAskPromise(promise, resolve) {
        return {
            promise, resolve,
        };
    }

    addAskPromise(hash) {
        const [promise, resolve] = getPromise();
        const ask = this.makeAskPromise(promise, resolve);
        this._promiseAskMap[hash] = ask;
        return ask;
    }

    deleteAskPromise(hash) {
        delete this._promiseAskMap[hash];
    }

    async ask(type, data) {
        const hash = getId();
        const ask = this.addAskPromise(hash);

        const timeoutError = new TimeoutError({
            timeout: this._askTimeout,
            onError: () => ask.resolve(),
        });

        timeoutError.start();
        this.push(serializeData({
            id: hash, type, data,
        }) + "\n");
        const result = await ask.promise;
        timeoutError.stop();

        this.deleteAskPromise(hash);

        return result;
    }

    async notify(type, data) {
        this.push(serializeData({
            type, data,
        }) + "\n");
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
                        this.onAsk(data);
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
        return this.push(data);
    }
}
