const TimeoutErrorTimer = require("../engine/TimeoutErrorTimer.js");
const FakeTimeout = require("./FakeTimeout.js");

module.exports = class FakeTimeoutErrorTimer extends TimeoutErrorTimer {
    constructor(_options) {
        const fakeTimeout = new FakeTimeout();
        const options = {
            ..._options,
            inject: {
                _setTimeout: fakeTimeout.getSetTimeout(),
                _clearTimeout: fakeTimeout.getClearTimeout(),
            },
        };
        super(options);
        this._fakeTimeout = fakeTimeout;
    }

    getFakeTimeout() {
        return this._fakeTimeout;
    }
}