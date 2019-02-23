const argv = require("yargs").argv;

// Engine

// TODO: Implement Error Catching (worker & manager -> connection or stream fail)
// TODO: Implement AccumulatorStream (using a stacking stream)

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
});

const manager = new ManagerService({
    connectionService,
});

// Mapping

worker.setMap("init", (key, send) => {
    let state = false;
    return async ({ data }) => {
        state = !state;
        await send("reduce_2_flows", state ? "final" : "final_alt", data);
    };
});

worker.setMap("reduce_2_flows", (key, send) => {
    return async ({ data }) => {
        await send("map", null, data);
    };
});

worker.setMap("map", (key, send) => {
    return async ({ data }) => {
        await send("final_reduce", "final", data);
    };
});

worker.setMap("final_reduce", (key) => {
    let sum = "";
    return [
        async ({ data }) => {
            console.log("line", key, data);
            sum += data + "\n";
        },
        () => {
            console.log(sum);
        },
    ];
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
    ).on("end", () => {
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
