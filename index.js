const fs = require("fs");
const path = require("path");
const argv = require('yargs').argv;

const config = require("./Config.js");
const ServerConnection = require("./ServerConnection.js");

const pool = {
    servers: config.servers.map((serverConfig) => new ServerConnection(serverConfig)),
    sendData: function (index, data) {
        this.servers[index].sendData(data);
    }
}

const StringToLinesTransform = require("./StringToLinesTransform.js");
const SendToProcessWritable = require("./SendToProcessWritable.js");

const stringToLinesStream = new StringToLinesTransform();
const sendToProcessStream = new SendToProcessWritable({
    pool,
});

// Test Part

const file = "data.txt";
const readFile = fs.createReadStream(file, { encoding: "utf8" })

readFile.pipe(stringToLinesStream);
stringToLinesStream.pipe(sendToProcessStream);

stringToLinesStream.on("data", data => {
    console.log("data", data.toString());
})
.on("end", () => {
    console.log("end");
});
