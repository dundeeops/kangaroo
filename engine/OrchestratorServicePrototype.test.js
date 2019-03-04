const {
    parseData,
} = require("./SerializationUtil.js");
const FakeConnectionService = require("./FakeConnectionService.js");
const OrchestratorServicePrototype = require("./OrchestratorServicePrototype.js");

const servicePrototypeFactory = async () => {
    const connectionService = new FakeConnectionService();
    await connectionService.start();
    const servicePrototype = new OrchestratorServicePrototype({
        connectionService,
    });
    return servicePrototype;
}

describe("OrchestratorServicePrototype", () => {
    test("should send", async () => {
        const servicePrototype = await servicePrototypeFactory();
        const serverName = await servicePrototype.send("testSession", "testGroup", "testStage", "testKey", {
            message: "testDataMessage",
        });
        const message = servicePrototype._connectionService.getConnection(serverName)._socket._fakeQueue[0];
        const data = parseData(message);
        expect(data.session).toBe("testSession");
        expect(data.group).toBe("testGroup");
        expect(data.stage).toBe("testStage");
        expect(data.key).toBe("testKey");
        expect(data.data.message).toBe("testDataMessage");
    });

    test("should ask", async () => {
        const servicePrototype = await servicePrototypeFactory();
        const data = await servicePrototype.ask("testAsk", {
            message: "testDataMessage",
        });
        expect(data.message).toBe("testDataMessage");
    });

    test("should notify", async () => {
        const servicePrototype = await servicePrototypeFactory();
        await servicePrototype.notify("testAsk", {
            message: "testDataMessage",
        });
        servicePrototype._connectionService.getConnections().forEach((connection) => {
            const data = connection._notifyQueue[0];
            expect(data.type).toBe("testAsk");
            expect(data.data.message).toBe("testDataMessage");
        });
    });
});
