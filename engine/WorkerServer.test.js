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

const workerServerFactory = (onAsk = jest.fn(), onData = jest.fn()) => {
    const workerServer = new WorkerServer({
        hostname: "test",
        port: 3333,
        getMappers: () => ["testStage"],
        onAsk, onData,
        inject: {
            _net: {
                createServer: (onConnect) => new FakeServer(onConnect),
            },
            _RestartService: FakeRestartService,
        }
    });
    return workerServer;
}

describe("WorkerServer", () => {
    test("should start", async () => {
        const workerServer = workerServerFactory();
        await workerServer.start();
    });
});
