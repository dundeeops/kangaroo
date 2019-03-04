const ConnectionService = require("../engine/ConnectionService.js");
const FakeTimeoutErrorTimer = require("./FakeTimeoutErrorTimer.js");
const FakeTimeout = require("./FakeTimeout.js");
const FakeAutoConnectionSocket = require("./FakeAutoConnectionSocket.js");

module.exports = class FakeConnectionService extends ConnectionService {
    constructor() {
        const fakeTimeout = new FakeTimeout();
        super({
            connections: [
                {
                    hostname: "test1",
                    port: 333,
                },
                {
                    hostname: "test2",
                    port: 334,
                },
            ],
            inject: {
                _setInterval: fakeTimeout.getSetTimeout(),
                _clearInterval: fakeTimeout.getClearTimeout(),
                _ConnectionSocket: FakeAutoConnectionSocket,
                _TimeoutErrorTimer: FakeTimeoutErrorTimer,
            }
        });
        this._fakeTimeout = fakeTimeout;
    }
}
