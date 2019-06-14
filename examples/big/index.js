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
    connections: config.servers
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
    hostname: config.hostname,
    port: config.port,
    connectionService,
});

const manager = new ManagerService({
    connectionService,
});

// Mapping

worker.setMapper("init", (key, send) => {
    let state = false;
    return async ({ data }) => {
        state = !state;
        await send("reduce_2_flows", state ? "final" : "final_alt", data);
    };
});

worker.setMapper("reduce_2_flows", (key, send) => {
    return async ({ data }) => {
        // await new Promise(r => setTimeout(r, 500));
        await send("map", null, data);
    };
});

worker.setMapper("map", (key, send) => {
    return async ({ data }) => {
        await send("final_reduce", "final", data);
    };
});

worker.setMapper("final_reduce", (key) => {
    let sum = 0;
    return [
        async ({ data }) => {
            sum++;
        },
        () => {
            console.log(sum);
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

    const timeout = new TimeoutErrorTimer();
    timeout.start("TIMEOUT: Error sending an initial stream");

    const max = 10000;
    let index = 0;
    manager.runStream(
        "init",
        null,
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
