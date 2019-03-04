const BaseDict = require("./BaseDict.js");
const {
    serializeData,
    parseData,
} = require("./SerializationUtil.js");
const {
    connectionSocketFactory,
    waitSocket,
} = require("./FakeConnectionSocket.js");

describe("ConnectionSocket", () => {

    test("should get hostname", async () => {
        const connectionSocket = connectionSocketFactory();
        expect(connectionSocket.getHostname()).toBe("test");
    });

    test("should get port", async () => {
        const connectionSocket = connectionSocketFactory();
        expect(connectionSocket.getPort()).toBe(3333);
    });

    test("should get name", async () => {
        const connectionSocket = connectionSocketFactory();
        expect(connectionSocket.getName()).toBe("test:3333");
    });

    test("should be disconnected", async () => {
        const connectionSocket = connectionSocketFactory();
        expect(connectionSocket.isAlive()).toBe(false);
    });

    test("should connect", async () => {
        let socket;
        const connectionSocket = connectionSocketFactory((_socket) => {
            socket = _socket;
        });
        await new Promise((r) => {
            connectionSocket.run(() => {});
            waitSocket(connectionSocket, () => socket, r);
        });
        expect(connectionSocket.isAlive()).toBe(true);
    });

    test("should check mapper", async () => {
        let socket;
        const connectionSocket = connectionSocketFactory((_socket) => {
            socket = _socket;
        });
        await new Promise((r) => {
            connectionSocket.run(() => {});
            waitSocket(connectionSocket, () => socket, r, ["test"]);
        });
        expect(connectionSocket.isContainsStage("test")).toBe(true);
        expect(connectionSocket.isContainsStage("testnon")).toBe(false);
    });

    test("should connect using service", async () => {
        let socket;
        const connectionSocket = connectionSocketFactory((_socket) => {
            socket = _socket;
        });
        await new Promise((r) => {
            connectionSocket.connect();
            waitSocket(connectionSocket, () => socket, r);
        });
        expect(connectionSocket.isAlive()).toBe(true);
    });

    test("should connect & destroy", async () => {
        let socket;
        const connectionSocket = connectionSocketFactory((_socket) => {
            socket = _socket;
        });
        await new Promise((r) => {
            connectionSocket.connect();
            waitSocket(connectionSocket, () => socket, r);
        });
        connectionSocket.destroy();
        expect(connectionSocket.isAlive()).toBe(false);
    });

    test("should connect & send", async () => {
        let socket;
        let expectingData = "test" + BaseDict.ENDING;
        let data;
        const connectionSocket = connectionSocketFactory((_socket) => {
            socket = _socket;
            socket.on("write", (_data) => {
                data = _data;
            });
        });
        await new Promise((r) => {
            connectionSocket.run(() => {});
            waitSocket(connectionSocket, () => socket, r);
        });
        connectionSocket.sendData(expectingData);
        expect(data).toBe(expectingData);
    });

    test("should accumulate, connect & then send", async () => {
        let socket;
        let expectingData = "test" + BaseDict.ENDING;
        let data;
        const connectionSocket = connectionSocketFactory((_socket) => {
            socket = _socket;
            socket.on("write", (_data) => {
                data = _data;
            });
        });
        connectionSocket.sendData(expectingData);
        await new Promise((r) => {
            waitSocket(connectionSocket, () => socket, r);
        });
        expect(data).toBe(expectingData);
    });

    test("should ask", async () => {
        let socket;
        const connectionSocket = connectionSocketFactory((_socket) => {
            socket = _socket;
            socket.on("write", (_data) => {
                const { id, type, data } = parseData(_data);
                socket.emit("data", serializeData({
                    id,
                    type,
                    data: {
                        response: data.message + type,
                    },
                }));
            });
        });
        await new Promise((r) => {
            connectionSocket.run(() => {});
            waitSocket(connectionSocket, () => socket, r);
        });
        const promiseAsk = connectionSocket.ask("test", {
            message: "test",
        });
        const result = await promiseAsk;
        expect(result.response).toBe("testtest");
    });
});
