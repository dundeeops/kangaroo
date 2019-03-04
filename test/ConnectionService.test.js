const ConnectionService = require("../engine/ConnectionService.js");
const {
    repeatIfTrueTimeout,
} = require("../engine/PromiseUtil.js");
const FakeTimeoutErrorTimer = require("./FakeTimeoutErrorTimer.js");
const FakeTimeout = require("./FakeTimeout.js");
const FakeAutoConnectionSocket = require("./FakeAutoConnectionSocket.js");

const connectionServiceFactory = (options) => {
    const fakeTimeout = new FakeTimeout();

    const connectionService = new ConnectionService({
        ...options,
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

    test("should start from connections", async () => {
        const [connectionService] = connectionServiceFactory({
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
        });
        await connectionService.start();
        expect(Array.from(connectionService._connectionsMap.keys()).length).toBe(2);
    });

    test("should start from poolingConnections", async () => {
        const [connectionService] = connectionServiceFactory({
            poolingConnections: [
                {
                    hostname: "test1",
                    port: 333,
                },
                {
                    hostname: "test2",
                    port: 334,
                },
            ],
        });
        await connectionService.start();
        expect(Array.from(connectionService._connectionsMap.keys()).length).toBe(2);
    });

    test("should start then add connection", async () => {
        const [connectionService] = connectionServiceFactory({});
        await connectionService.start();
        expect(Array.from(connectionService._connectionsMap.keys()).length).toBe(0);
        await connectionService.addConnectionAndConnect("test2", 334);
        expect(Array.from(connectionService._connectionsMap.keys()).length).toBe(1);
    });

    test("should start then add poolingConnections", async () => {
        const [connectionService, fakeTimeout] = connectionServiceFactory({});
        await connectionService.start();
        expect(Array.from(connectionService._connectionsMap.keys()).length).toBe(0);
        connectionService.addPoolConnection({
            hostname: "test2",
            port: 334,
        });
        fakeTimeout.simulateOnTick();
        await new Promise((r) => repeatIfTrueTimeout(() => {
            if (Array.from(connectionService._connectionsMap.keys()).length === 1) {
                r();
                return false;
            } else {
                return true;
            }
        }, 50));
        expect(Array.from(connectionService._connectionsMap.keys()).length).toBe(1);
    });

});
