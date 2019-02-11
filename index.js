const fs = require("fs");
const path = require("path");
const { pipeline } = require('stream');

const config = require("./Config.js");
const StringToLinesTransform = require("./StringToLinesTransform.js");
const MapReduceOrchestrator = require("./MapReduceOrchestrator.js");

const serverPool = new ServerPool({
    config
});

// const { ServerFactory } = require("./ServerFactory.js");

// ServerFactory({
//     port: 1337,
//     hostname: "0.0.0.0",
//     prepare: socket => {
//         socket.write("Echo server\r\n");
//         socket.on("data", function(data) {
//             console.log(data);
//             textChunk = data.toString("utf8");
//             console.log(textChunk);
//             // socket.write(textChunk);
//         });
//     }
// });

const mapReduceOrchestrator = new MapReduceOrchestrator({
    serverPool,
    server: null,
    initStage: "init",
    config,
});


const { Transform, Writable } = require("stream");

mapReduce.map("init", (key) => {
    const stringToLinesStream = new Transform({
        transform(chunk, encoding, callback) {
            this.push(chunk.toString());
            callback();
        }
    });
    return ["final", "final", stringToLinesStream];
});

mapReduce.map("final", (key) => {
    let data = "";
    const stringToLinesStream = new Writable({
        write(chunk, encoding, callback) {
            data += chunk.toString();
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

const file = "./data.txt";
const readFileStream = fs.createReadStream(path.resolve(file), { encoding: "utf8" });
const stringToLinesStream = new StringToLinesTransform();

pipeline(
    readFileStream,
    stringToLinesStream,
    mapReduceOrchestrator.getStream(),
    err => {
        if (err) {
            console.error("Pipeline failed.", err);
        } else {
            console.log("Pipeline succeeded.");
        }
    }
);

// stringToLinesStream.on("data", data => {
//     console.log("data", data.toString());
// })
// .on("end", () => {
//     console.log("end");
// });
