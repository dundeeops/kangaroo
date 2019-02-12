const config = require("./Config.js");

// Engine

const ServerPool = require("./engine/ServerPool.js");
const ServerEntry = require("./engine/ServerEntry.js");
const MapTransform = require("./engine/MapTransform.js");
const MapWritable = require("./engine/MapWritable.js");
const MapReduceWorker = require("./engine/MapReduceWorker.js");
const MapReduceManager = require("./engine/MapReduceManager.js");

// Initialization

const serverPool = new ServerPool({
    servers: config.servers
});

const server = new ServerEntry({
    name: config.name,
    hostname: config.hostname,
    port: config.port,
});

const mapReduceWorker = new MapReduceWorker({
    server,
    serverPool,
    preferableServerName: config.name,
});

const mapReduceManager = new MapReduceManager({
    serverPool,
    preferableServerName: config.name,
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

            callback();

            // process.exit();

            rl.question("\n", (answer) => {
                if (answer === "r") {
                    run();
                } else {
                    rl.close();
                    process.exit();
                }
            });
        }
    });
});

// Run Worker

mapReduceWorker.runWorker();

// Test Part

const fs = require("fs");
const path = require("path");

function run() {
    mapReduceManager.runManagerStream(
        "init",
        fs.createReadStream(path.resolve("./data.txt"), { encoding: "utf8" }),
    ).on("finish", () => {
        console.log("The data has been sent!");
    });
    // mapReduceManager.runManagerStream(
    //     "init",
    //     fs.createReadStream(path.resolve("./data.txt"), { encoding: "utf8" }),
    // ).on("finish", () => {
    //     console.log("The data has been sent!");
    // });
    // mapReduceManager.runManagerStream(
    //     "init",
    //     fs.createReadStream(path.resolve("./data.txt"), { encoding: "utf8" }),
    // ).on("finish", () => {
    //     console.log("The data has been sent!");
    // });
}

run();
