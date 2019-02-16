const config = require("./Config.js");
const argv = require('yargs').argv;

// Engine

// TODO: Implement Error Catching (worker & manager -> connection or stream fail)
// TODO: Implement AccumulatorStream (using a stacking stream)

const ConnectionService = require("./engine/ConnectionService.js");
const MapTransform = require("./engine/MapTransformStream.js");
const MapWritable = require("./engine/MapWritableStream.js");
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

worker.setStream("init", (key) => {
    let state = false;
    return new MapTransform({
        transform(chunk, encoding, callback) {
            const { data } = this.parse(chunk);
            state = !state;
            this.send({
                stage: "final",
                key: state ? "final" : "final_alt",
                data,
            });
            callback();
        },
        final(callback) {
            // console.log("The data has been processed 1 stage!", key);
            this.send({
                stage: "final",
                key: "final",
                data: null,
            });
            this.send({
                stage: "final",
                key: "final_alt",
                data: null,
            });
            callback();
        }
    });
});

worker.setStream("final", (key) => {
    let sum = "";
    return new MapWritable({
        write(chunk, encoding, callback) {
            const { data } = this.parse(chunk);
            console.log("line", key, data);
            sum += data + "\n";
            callback();
        },
        final(callback) {
            console.log(sum);
            console.log("The data has been processed!", key);

            callback();
        }
    });
});

// Run Worker

worker.run();

// Test Part

const fs = require("fs");
const path = require("path");

async function run() {
    await connectionService.run();

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
