const FakeTimeoutErrorTimer = require("./FakeTimeoutErrorTimer.js");
const FakeRestartService = require("./FakeRestartService.js");
const ConnectionSocket = require("./ConnectionSocket.js");
const FakeAutoSocket = require("./FakeAutoSocket.js");

module.exports = class FakeAutoConnectionSocket extends ConnectionSocket {
    constructor(options) {
        super({
            hostname: "test",
            port: 3333,
            inject: {
                _net: {
                    Socket: FakeAutoSocket,
                },
                _TimeoutErrorTimer: FakeTimeoutErrorTimer,
                _RestartService: FakeRestartService,
            },
            ...options,
        });
    }
}
