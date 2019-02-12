const fs = require("fs");
const path = require("path");

const config = require("./Config.js");
const MapReduceOrchestrator = require("./MapReduceOrchestrator.js");
const ServerPool = require("./ServerPool.js");
const ServerEntry = require("./ServerEntry.js");

const serverPool = new ServerPool({
    servers: config.servers
});

const server = new ServerEntry({
    name: config.name,
    hostname: config.hostname,
    port: config.port,
});

const mapReduceOrchestrator = new MapReduceOrchestrator({
    serverPool,
    server,
    initStage: "init",
});

const { Transform, Writable } = require("stream");

mapReduceOrchestrator.map("init", (key) => {
    const stringToLinesStream = new Transform({
        transform(chunk, encoding, callback) {
            // TODO: chunk be an object and put stage & key inside a stream extends MapTransform & MapWritable
            // this.push("final", "final", chunk); | this.push("final", chunk); | this.push(chunk);
            this.push(chunk.toString());
            callback();
        }
    });
    return ["final", "final", stringToLinesStream];
});

const readline = require("readline");
const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

mapReduceOrchestrator.map("final", (key) => {
    let data = "";
    const stringToLinesStream = new Writable({
        write(chunk, encoding, callback) {
            console.log("line", chunk.toString());
            data += chunk.toString() + "\n";
            callback();
        },
        final(callback) {
            console.log(data);
            console.log("The data has been processed!");

            rl.question("Run again? [y/n]: ", (answer) => {
                if (answer === "y") {
                    run();
                } else {
                    rl.close();
                    process.exit();
                }
            });

            callback();
        }
    });
    return [null, null, stringToLinesStream];
});

mapReduceOrchestrator.runWorker();

// Test Part

function run() {
    mapReduceOrchestrator.setManagerStream(
        fs.createReadStream(path.resolve("./data.txt"), { encoding: "utf8" })
    ).on("finish", () => {
        console.log("The data has been sent!");
    });
}

run();
