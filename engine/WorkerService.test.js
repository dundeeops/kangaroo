const WorkerService = require("./WorkerService.js");
const FakeWorkerServer = require("./FakeWorkerServer.js");

const workerServiceFactory = () => {
    const workerService = new WorkerService({
        hostname: "test",
        port: 3333,
        inject: {
            _WorkerServer: FakeWorkerServer,
        }
    });
    return workerService;
}

describe("WorkerService", () => {
    test("should start", async () => {
        const workerService = workerServiceFactory();
        await workerService.start();
    });
});
