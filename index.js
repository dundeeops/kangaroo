const fs = require("fs");
const path = require("path");
const { pipeline } = require('stream');

const config = require("./Config.js");
const StringToLinesTransform = require("./StringToLinesTransform.js");
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
    name: config.name,
    initStage: "init",
});

const { Transform, Writable } = require("stream");

mapReduceOrchestrator.map("init", (key) => {
    const stringToLinesStream = new Transform({
        transform(chunk, encoding, callback) {
            this.push(chunk.toString());
            callback();
        }
    });
    return ["final", "final", stringToLinesStream];
});

mapReduceOrchestrator.map("final", (key) => {
    let data = "";
    const stringToLinesStream = new Writable({
        write(chunk, encoding, callback) {
            data += chunk.toString() + "\n";
            callback();
        },
        final(callback) {
            console.log(data);
            callback();
        }
    });
    return [null, null, stringToLinesStream];
});

// Test Part

async function run() {
    await mapReduceOrchestrator.init();

    const file = "./data.txt";
    const readFileStream = fs.createReadStream(path.resolve(file), { encoding: "utf8" });
    const stringToLinesStream = new StringToLinesTransform();
    
    pipeline(
        readFileStream,
        stringToLinesStream,
        mapReduceOrchestrator.getOutcomeStream(),
        err => {
            if (err) {
                console.error("Pipeline failed.", err);
            } else {
                console.log("Pipeline succeeded.");
            }
        }
    );
}

run();

// stringToLinesStream.on("data", data => {
//     console.log("data", data.toString());
// })
// .on("end", () => {
//     console.log("end");
// });
