const EventEmitter = require("events");
const WorkerServer = require("./WorkerServer.js");
const FakeSocket = require("./FakeSocket.js");
const FakeRestartService = require("./FakeRestartService.js");

class FakeServer extends EventEmitter {
    constructor(onConnect) {
        super();
        this._onConnect = onConnect;
    }

    listen(port, hostname, onReady) {
        onReady();
        this.fakeSocket();
    }

    fakeSocket() {
        const socket = new FakeSocket();
        this._onConnect(socket);
    }

    pipe() {
        return this;
    }
}

module.exports = class FakeWorkerServer extends WorkerServer  {
    constructor(options) {
        super({
            hostname: "test",
            port: 3333,
            getMappers: () => ["testStage"],
            inject: {
                _net: {
                    createServer: (onConnect) => new FakeServer(onConnect),
                },
                _RestartService: FakeRestartService,
            },
            ...options,
        });
    }
}
