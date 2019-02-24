const {
    getId,
} = require("./SerializationUtil");

const setTimeoutFactory = (cb) => (fn, timeout) => {
    const id = getId();
    cb(id, fn, timeout);
    return id;
};

const clearTimeoutFactory = (cb) => (id) => {
    cb(id);
};

module.exports = class FakeTimeout {
    constructor() {
        this._id;
        this._fn;
        this._timeout;
        this._isStared = false;
        this._isStopped = false;
        this._setTimeout = setTimeoutFactory(this.onStart.bind(this));
        this._clearTimeout = clearTimeoutFactory(this.onStop.bind(this));
    }

    onStart(id, fn, timeout) {
        this._id = id;
        this._fn = fn;
        this._timeout = timeout;
        this._isStared = true;
        this._isStopped = false;
    }

    onStop() {
        this._isStopped = true;
    }

    getSetTimeout() {
        return this._setTimeout;
    }

    getClearTimeout() {
        return this._clearTimeout;
    }

    getState() {
        return {
            id: this._id,
            fn: this._fn,
            timeout: this._timeout,
            isStared: this._isStared,
            isStopped: this._isStopped,
        }
    }

    getState() {
        return {
            id: this._id,
            fn: this._fn,
            timeout: this._timeout,
            isStared: this._isStared,
            isStopped: this._isStopped,
        }
    }

    simulateOnTick() {
        return this._fn();
    }
}
