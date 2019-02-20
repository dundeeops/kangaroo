const config = require("./Config.js");
const argv = require('yargs').argv;

// Engine

// TODO: Implement Error Catching (worker & manager -> connection or stream fail)
// TODO: Implement AccumulatorStream (using a stacking stream)

const ConnectionService = require("./engine/ConnectionService.js");
const WorkerService = require("./engine/WorkerService.js");
const ManagerService = require("./engine/ManagerService.js");
const TimeoutErrorTimer = require("./engine/TimeoutErrorTimer.js");

// Exit if "entrr" or "q" are pressed

const readline = require("readline");
readline.emitKeypressEvents(process.stdin);
process.stdin.setRawMode(true);
process.stdin.on("keypress", (str, key) => {
    if (key.name === "q" || key.name === "return") {
        process.exit();
    }
});

// Initialization

const connectionService = new ConnectionService({
    poolingConnections: config.servers
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
    server: {
        hostname: config.hostname,
        port: config.port,
    },
    connectionService,
    preferableServerName: config.preferableServerName,
});

const manager = new ManagerService({
    connectionService,
    preferableServerName: config.preferableServerName,
});

// Mapping

worker.setStream("init", (key, send) => {
    let state = false;
    return async ({ data, eof }) => {
        if (!eof) {
            state = !state;
            await send("reduce_2_flows", state ? "final" : "final_alt", data);
        }
    };
});

worker.setStream("reduce_2_flows", (key, send) => {
    return async ({ data, eof }) => {
        if (!eof) {
            await send("map", null, data);
        }
    };
});

worker.setStream("map", (key, send) => {
    return async ({ data, eof }) => {
        if (!eof) {
            await send("final_reduce", "final", data);
        }
    };
});

worker.setStream("final_reduce", (key) => {
    let sum = "";
    return async ({ data, eof }) => {
        if (!eof) {
            console.log("line", key, data);
            sum += data + "\n";
        } else {
            console.log("final", sum);
            console.log("The data has been processed!", key);
        }
    };
});

// Run Worker

worker.start();

// Test Part

const fs = require("fs");
const path = require("path");

async function run() {
    await connectionService.start();

    let timeout = new TimeoutErrorTimer();
    timeout.start("TIMEOUT: Error sending an initial stream");

    manager.runStream(
        "init",
        fs.createReadStream(path.resolve("./data.txt"), { encoding: "utf8" }),
    ).on("finish", () => {
        console.log("The data has been sent!");
        timeout.stop();
    });

    // manager.runStream(
    //     "init",
    //     process.stdin,
    // ).on("finish", () => {
    //     console.log("The data has been sent!");
    //     timeout.stop();
    // });

    // manager.runStream(
    //     "init",
    //     fs.createReadStream(path.resolve("./data.txt"), { encoding: "utf8" }),
    // ).on("finish", () => {
    //     console.log("The data has been sent!");
    // });

    // manager.runStream(
    //     "init",
    //     fs.createReadStream(path.resolve("./data.txt"), { encoding: "utf8" }),
    // ).on("finish", () => {
    //     console.log("The data has been sent!");
    // });
}

run();
