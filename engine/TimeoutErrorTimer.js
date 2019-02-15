const TIMEOUT = 10000;
const TIMEOUT_MESSAGE = "TIMEOUT ERROR";

module.exports = class TimeoutErrorTimer {
    constructor(options) {
        this._timeout = null;
        this._timeoutInterval = (options && options.timeout) || TIMEOUT;
        this._timeoutMessage = (options && options.message) || TIMEOUT_MESSAGE;
    }

    start(message, timeout) {
        if (!this._timeout) {
            this._timeout = setTimeout(() => {
                throw Error(message || this._timeoutMessage);
            }, timeout || this._timeoutInterval);
        }
    }

    stop() {
        if (this._timeout) {
            clearTimeout(this._timeout);
        }
        this._timeout = null;
    }
}
