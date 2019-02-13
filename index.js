const config = require("./Config.js");
const argv = require('yargs').argv;

// Engine

const ServerPool = require("./engine/ServerPool.js");
const MapTransform = require("./engine/MapTransform.js");
const MapWritable = require("./engine/MapWritable.js");
const MapReduceWorker = require("./engine/MapReduceWorker.js");
const MapReduceManager = require("./engine/MapReduceManager.js");
const TimeoutError = require("./engine/TimeoutError.js");

// Initialization

const serverPool = new ServerPool({
    servers: config.servers
});

if (argv.c) {
    argv.c.forEach((name) => {
        if (typeof name === "string") {
            const [hostname, port] = name.split(":");
            serverPool.addServer(hostname, port);
        }
    });
}

const mapReduceWorker = new MapReduceWorker({
    server: {
        hostname: config.hostname,
        port: config.port,
    },
    serverPool,
    preferableServerName: config.preferableServerName,
});

const mapReduceManager = new MapReduceManager({
    serverPool,
    preferableServerName: config.preferableServerName,
});

// Mapping

mapReduceWorker.setMap("init", (key) => {
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

const readline = require("readline");
const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

let count = 0;

mapReduceWorker.setMap("final", (key) => {
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
            count++;

            callback();

            if (count === 2) {
                process.exit();

                // rl.question("\n", (answer) => {
                //     if (answer === "r") {
                //         run();
                //     } else {
                //         rl.close();
                //         process.exit();
                //     }
                // });
            }
        }
    });
});

// Run Worker

mapReduceWorker.run();

// Test Part

const fs = require("fs");
const path = require("path");

async function run() {
    await serverPool.run();

    let timeout = new TimeoutError();
    timeout.start("TIMEOUT: Error sending an initial stream");

    mapReduceManager.runStream(
        "init",
        fs.createReadStream(path.resolve("./data.txt"), { encoding: "utf8" }),
    ).on("finish", () => {
        console.log("The data has been sent!");
        timeout.stop();
    });

    // mapReduceManager.runStream(
    //     "init",
    //     fs.createReadStream(path.resolve("./data.txt"), { encoding: "utf8" }),
    // ).on("finish", () => {
    //     console.log("The data has been sent!");
    // });

    // mapReduceManager.runStream(
    //     "init",
    //     fs.createReadStream(path.resolve("./data.txt"), { encoding: "utf8" }),
    // ).on("finish", () => {
    //     console.log("The data has been sent!");
    // });
}

run();
