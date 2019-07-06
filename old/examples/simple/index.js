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
    return async ({ data }) => {
        console.log('init', data);
        
        state = !state;
        await send("reduce_2_flows", state ? "final" : "final_alt", data);
    };
});

worker.setMapper("reduce_2_flows", (key, send) => {
    return async ({ data }) => {
        console.log('reduce_2_flows', data);
        
        await send("map", null, data);
    };
});

worker.setMapper("map", (key, send) => {
    return async ({ data }) => {
        console.log('map', data);
    
        await send("final_reduce", "final", data);
    };
});

worker.setMapper("final_reduce", (key) => {
    let sum = "";
    return [
        async ({ data }) => {
            console.log("line", key, data);
            sum += data + "\n";
        },
        () => {
            console.log('All processed!');
            console.log(sum);
        },
    ];
});

// Test Part

const fs = require("fs");
const path = require("path");

async function run() {
    await worker.start();
    await connectionService.start();
    const timeout = new TimeoutErrorTimer();
    timeout.start("TIMEOUT: Error sending an initial stream");

    manager.runStream(
        "init",
        null,
        fs.createReadStream(path.resolve("./data.txt"), { encoding: "utf8" }),
    ).on("end", () => {
        console.log("The data has been sent! Processing...");
        timeout.stop();
    });
}

run();
