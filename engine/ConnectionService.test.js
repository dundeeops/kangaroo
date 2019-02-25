const FakeTimeoutErrorTimer = require("./FakeTimeoutErrorTimer.js");
const FakeRestartService = require("./FakeRestartService.js");
const FakeTimeout = require("./FakeTimeout.js");
const Dict = require("./AskDict.js");
const ConnectionService = require("./ConnectionService.js");
const {
    serializeData,
    deserializeData,
} = require("./SerializationUtil.js");
const {
    startUnlessTimeout,
} = require("./PromisifyUtil.js");
const FakeAutoConnectionSocket = require("./FakeAutoConnectionSocket.js");

const connectionServiceFactory = (onConnect) => {
    const fakeTimeout = new FakeTimeout();

    const connectionService = new ConnectionService({
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

    return [connectionService, fakeTimeout];
}

describe("ConnectionService", () => {

    test("should start", async () => {
        const [connectionService] = connectionServiceFactory();
        await connectionService.start();
        expect(Array.from(connectionService._connectionsMap.keys()).length).toBe(2);
    });

});
