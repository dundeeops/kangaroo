const {
    getId,
} = require("./SerializationUtil");
const FakeTimeout = require("./FakeTimeout.js");
const TimeoutErrorTimer = require("./TimeoutErrorTimer.js");

export class FakeTimeoutErrorTimer extends TimeoutErrorTimer {
    constructor(_options) {
        this._fakeTimeout = new FakeTimeout();
        const options = {
            ..._options,
            inject: {
                _setTimeout: fakeTimeout.getSetTimeout(),
                _clearTimeout: fakeTimeout.getClearTimeout(),
            },
        };
        super(options);
    }

    getFakeTimeout() {
        return this._fakeTimeout;
    }
}