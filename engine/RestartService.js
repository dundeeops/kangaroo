const domain = require("domain");
const TimeoutErrorTimer = require("./TimeoutErrorTimer.js");

const RESTART_TIMEOUT = 5000;
const DEFAULT_TIMEOUT_ERROR_MESSAGE = "TIMEOUT: Error starting a service";

const defaultOptions = {
    restart: true,
    restartTimeout: RESTART_TIMEOUT,
    run: (c) => c(),
    isAlive: () => true,
    onError: () => {},
    onErrorTimeout: () => {},
    timeout: {},
    timeoutErrorMessage: DEFAULT_TIMEOUT_ERROR_MESSAGE,
    inject: {
        _setInterval: setInterval,
        _clearInterval: clearInterval,
        _domainCreate: domain.create,
        _TimeoutErrorTimer: TimeoutErrorTimer,
    },
};

module.exports = class RestartService {
    constructor(_options = {}) {
        const options = {
            ...defaultOptions,
            ..._options,
            inject: {
                ...defaultOptions.inject,
                ..._options.inject,
            },
        };

        this._shouldRestart = options.restart;
        this._restartTimeout = options.restartTimeout;
        this._run = options.run;
        this._isAlive = options.isAlive;
        this._onError = options.onError;
        this._onErrorTimeout = options.onErrorTimeout;

        this.initInjections(options);

        this.init(options);
        this.initDomain();
        this.initTimeoutErrorTimer(options);
    }

    initInjections(options) {
        this._setInterval = options.inject._setInterval;
        this._clearInterval = options.inject._clearInterval;
        this._domainCreate = options.inject._domainCreate;
        this._TimeoutErrorTimer = options.inject._TimeoutErrorTimer;
    }

    init() {
        this._isStarting = false;
        this._restartInterval = null;
    }

    initDomain() {
        this._domain = this._domainCreate();
        this._domain.on("error", this.onError.bind(this));
    }

    initTimeoutErrorTimer(options) {
        this._timeoutError = new this._TimeoutErrorTimer({
            ...options.timeout,
            onError: this.onErrorTimeout.bind(this),
            message: options.timeoutErrorMessage,
        });
    }

    setRestart(value) {
        this._shouldRestart = value;
    }

    setOnErrorTimeout(value) {
        this._onErrorTimeout = value;
    }

    onError(error) {
        this._onError(error);
        if (!this._isAlive()) {
            this._isStarting = false;
            this.startRestart();
        }
    }

    onErrorTimeout(error) {
        this._onErrorTimeout(error);
    }

    startRestart() {
        this._timeoutError.start();
        this.stopRestart();
        this._restartInterval = this._setInterval(() => {
            if (this._shouldRestart) {
                this.start();
            }
        }, this._restartTimeout);
    }

    stopRestart() {
        if (this._restartInterval) {
            this._clearInterval(this._restartInterval);
            this._restartInterval = null;
        }
    }

    checkIsAlive() {
        if (!this._isAlive()) {
            this.startRestart();
        } else {
            this.stopRestart();
            this._timeoutError.stop();
        }
    }

    start() {
        if (!this._isStarting && !this._isAlive()) {
            this._timeoutError.start();
            return this.run(() => {
                this._isStarting = false;
                this.checkIsAlive();
            });
        }
    }

    run() {
        this._domain.run(() => {
            this._isStarting = true;

            this._run(() => {
                this._isStarting = false;
                this.checkIsAlive();
            });
        });
    }
}
