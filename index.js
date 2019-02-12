const config = require("./Config.js");
const MapReduceOrchestrator = require("./MapReduceOrchestrator.js");
const ServerPool = require("./ServerPool.js");
const ServerEntry = require("./ServerEntry.js");
const MapTransform = require("./MapTransform.js");
const MapWritable = require("./MapWritable.js");

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
});

mapReduceOrchestrator.map("init", (key) => {
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

// const readline = require("readline");
// const rl = readline.createInterface({
//     input: process.stdin,
//     output: process.stdout
// });

mapReduceOrchestrator.map("final", (key) => {
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

            // rl.question("Run again? [y/n]: ", (answer) => {
            //     if (answer === "y") {
            //         run();
            //     } else {
            //         rl.close();
            //         process.exit();
            //     }
            // });
        }
    });
});

mapReduceOrchestrator.runWorker();

// Test Part

const fs = require("fs");
const path = require("path");

function run() {
    mapReduceOrchestrator.setManagerStream(
        "init",
        fs.createReadStream(path.resolve("./data.txt"), { encoding: "utf8" }),
    ).on("finish", () => {
        console.log("The data has been sent!");
    });
    // mapReduceOrchestrator.setManagerStream(
    //     "init",
    //     fs.createReadStream(path.resolve("./data.txt"), { encoding: "utf8" }),
    // ).on("finish", () => {
    //     console.log("The data has been sent!");
    // });
    // mapReduceOrchestrator.setManagerStream(
    //     "init",
    //     fs.createReadStream(path.resolve("./data.txt"), { encoding: "utf8" }),
    // ).on("finish", () => {
    //     console.log("The data has been sent!");
    // });
}

run();
