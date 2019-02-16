const domain = require("domain");
const TimeoutError = require("./TimeoutErrorTimer.js");

const RESTART_TIMEOUT = 5000;

module.exports = class RestartService {
    constructor(options) {
        this._isStarting = false;
        this._restartInterval = null;

        this._domain = domain.create();
        this._domain.on("error", this.onError.bind(this));

        this._timeoutError = new TimeoutError({
            ...(options.timeout || {}),
            onError: this.onErrorTimeout.bind(this),
            message: options.timeoutErrorMessage || "TIMEOUT: Error starting a service"
        });

        this._shouldRestart = options.restart != null ? options.restart : true;
        this._restartTimeout = options.restartTimeout || RESTART_TIMEOUT;

        this._run = options.run || ((c) => c());
        this._isAlive = options.isAlive || (() => true);
        this._onError = options.onError || (() => {});
        this._onErrorTimeout = options.onErrorTimeout || (() => {});
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
        this._restartInterval = setInterval(() => {
            if (this._shouldRestart) {
                this.start();
            }
        }, this._restartTimeout);
    }

    stopRestart() {
        if (this._restartInterval) {
            clearInterval(this._restartInterval);
            this._restartInterval = null;
        }
    }

    start() {
        if (!this._isStarting && !this._isAlive()) {
            this._timeoutError.start();
            return this.run(() => {
                this._isStarting = false;
                if (!this._isAlive()) {
                    this.startRestart();
                } else {
                    this.stopRestart();
                    this._timeoutError.stop();
                }
            });
        }
    }

    run() {
        this._domain.run(() => {
            this._isStarting = true;

            this._run(() => {
                this._isStarting = false;
            });
        });
    }
}
