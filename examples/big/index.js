const argv = require("yargs").argv;

// Engine

const {
    ConnectionService,
    WorkerService,
    ManagerService,
    TimeoutErrorTimer,
} = require("../../index.js");

// Exit if "enter" or "q" are pressed

const readline = require("readline");
readline.emitKeypressEvents(process.stdin);
process.stdin.setRawMode(true);
process.stdin.on("keypress", (str, key) => {
    if (key.name === "q" || key.name === "return") {
        process.exit();
    }
});

// Initialization

const {config} = require("./Config.js");

const connectionService = new ConnectionService({
    connections: config.servers,
    onConnectError: console.error,
    onConnectTimeoutError: console.error,
});

if (argv.c) {
    argv.c.forEach((name) => {
        if (typeof name === "string") {
            const [hostname, port] = name.split(":");
            connectionService.addServer(hostname, port);
        }
    });

    argv.p.forEach((name) => {
        if (typeof name === "string") {
            const [hostname, port] = name.split(":");
            connectionService.addServer(hostname, port);
        }
    });
}

const worker = new WorkerService({
    managerServer: config.managerServer,
    dataServer: config.dataServer,
    key: config.key,
    connectionService,
    onError: console.error,
    onErrorTimeout: console.error,
});

const manager = new ManagerService({
    connectionService,
    onError: console.error,
});

// Mapping

worker.setMapper("init", (key, send) => {
    let state = false;
    return [
        async ({ data }) => {
            state = !state;
            await send("reduce_2_flows", state ? "final" : "final_alt", data);
        },
        () => {
            // console.log('Finished! init');
        },
    ];
});

worker.setMapper("reduce_2_flows", (key, send) => {
    return [
        async ({ data }) => {
            // await new Promise(r => setTimeout(r, 500));
            await send("map", null, data);
        },
        () => {
            console.log('Finished 2 flows!');
        },
    ];
});

worker.setMapper("map", (key, send) => {
    return [
        async ({ data }) => {
            await send("final_reduce", "final", data);
        },
        () => {
            // console.log('Finished map!');
        },
    ];
});

const timeStarted = +new Date();
worker.setMapper("final_reduce", (key) => {
    let sum = 0;
    return [
        async ({ data }) => {
            sum++;
            // console.log(sum);
        },
        () => {
            const timePassed = (new Date() - timeStarted) / 1000;
            const minutes = Math.floor(timePassed / 60);
            const seconds = Math.floor(timePassed % 60);
            console.log('Finished!', sum, `${minutes} min`, `${seconds} sec`);
        },
    ];
});

// Test Part

const fs = require("fs");
const path = require("path");
const stream = require("stream");

async function run() {
    await worker.start();
    await connectionService.start();

    const timeout = new TimeoutErrorTimer({
        timeout: 1000000,
    });
    timeout.start("TIMEOUT: Error sending an initial stream");

    const max = 100000;
    let index = 0;
    manager.runStream(
        "init",
        null,
        // fs.createReadStream(path.resolve("./big_data.txt"), { encoding: "utf8" }),
        new stream.Readable({
            read() {
                if (max <= index) {
                    this.push(null);
                } else {
                    this.push(Buffer.from(String(index) + "\n", 'utf8'));
                    index++;
                }
            }
        }),
    ).on("end", () => {
        console.log("The data has been sent! Processing...");
        timeout.stop();
    });
}

run();
