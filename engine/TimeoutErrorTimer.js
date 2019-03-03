const TIMEOUT = 10000;
const TIMEOUT_MESSAGE = "TIMEOUT ERROR";

const defaultOptions = {
    onError: null,
    message: TIMEOUT_MESSAGE,
    timeout: TIMEOUT,
    inject: {
        _setTimeout: setTimeout,
        _clearTimeout: clearTimeout,
    },
};

module.exports = class TimeoutErrorTimer {
    constructor(_options = {}) {
        const options = {
            ...defaultOptions,
            ..._options,
            inject: {
                ...defaultOptions.inject,
                ..._options.inject,
            },
        };

        this._timeoutInterval = options.timeout;
        this._timeoutMessage = options.message;
        this._onError = options && options.onError;

        this.initInjections(options);

        this.init(options);
    }

    initInjections(options) {
        this._setTimeout = options.inject._setTimeout;
        this._clearTimeout = options.inject._clearTimeout;
    }

    init() {
        this._timeout = null;
    }

    start(message, timeout = this._timeoutInterval) {
        if (!this._timeout) {
            this._timeout = this._setTimeout(() => {
                this.onTimeout(message);
            }, timeout);
        }
    }

    onTimeout(_message) {
        const message = _message || this._timeoutMessage;
        const error = Error(message);

        if (this._onError) {
            this._onError(error);
        } else {
            throw error;
        }
    }

    stop() {
        if (this._timeout) {
            this._clearTimeout(this._timeout);
        }
        this._timeout = null;
    }
}
