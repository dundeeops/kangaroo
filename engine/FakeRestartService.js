const FakeTimeout = require("./FakeTimeout.js");
const RestartService = require("./RestartService.js");
const FakeTimeoutErrorTimer = require("./FakeTimeoutErrorTimer.js");

module.exports = class FakeRestartService extends RestartService {
    constructor(_options) {
        const fakeTimeout = new FakeTimeout();
        const options = {
            ..._options,
            inject: {
                _setInterval: fakeTimeout.getSetTimeout(),
                _clearInterval: fakeTimeout.getClearTimeout(),
                _domainCreate: () => ({
                    run: (fn) => {
                        fn();
                    },
                    on: (type) => {
                        if (_options.fakeOnDomain) {
                            _options.fakeOnDomain(type);
                        }
                    },
                }),
                _TimeoutErrorTimer: FakeTimeoutErrorTimer,
            },
        };
        super(options);
        this._fakeTimeout = fakeTimeout;
    }

    getFakeTimeout() {
        return this._fakeTimeout;
    }
}