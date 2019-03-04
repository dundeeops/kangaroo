const ConnectionSocket = require("../engine/ConnectionSocket.js");
const FakeTimeoutErrorTimer = require("./FakeTimeoutErrorTimer.js");
const FakeRestartService = require("./FakeRestartService.js");
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
        this._askQueue = [];
        this._notifyQueue = [];
    }

    async ask(type, data) {
        if (type === "testAsk") {
            return data;
        }
        this._askQueue.push({type, data});
        return null;
    }

    async notify(type, data) {
        this._notifyQueue.push({type, data});
    }
}
