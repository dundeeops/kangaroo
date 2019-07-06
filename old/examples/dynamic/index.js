const argv = require("yargs").argv;
const tar = require("tar");

// Engine

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
    connections: config.servers
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
    hostname: config.hostname,
    port: config.port,
    connectionService,
});

const manager = new ManagerService({
    connectionService,
});

// Static Mapping

worker.setMapper("static", (key) => {
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

// Test Part

const fs = require("fs");
const path = require("path");

async function run() {
    await worker.start();
    await connectionService.start();

    process.stdin.on("keypress", async (str, key) => {
        if (key.name === "l") {
            const timeout = new TimeoutErrorTimer();
            timeout.start("TIMEOUT: Error uploading");
            manager.uploadModuleStream(
                tar.c({
                    gzip: true,
                }, ["./entry.js", "./package.json"]),
                {
                    id: "test",
                    mappers: {
                        "entry.js": ["test"],
                    },
                },
            ).on("end", async () => {
                console.log("The uploading completed! Unzipping...");
                timeout.stop();
            });
        } else if (key.name === "o") {
            const statuses = await manager.getStaticModulesStatus("test");
            console.log(statuses);
        } else if (key.name === "k") {
            const timeout = new TimeoutErrorTimer();
            timeout.start("TIMEOUT: Error sending an initial stream");
            manager.runStream(
                "test",
                "hello",
                fs.createReadStream(path.resolve("./data.txt"), { encoding: "utf8" })
            ).on("end", () => {
                console.log("The data has been sent! Processing...");
                timeout.stop();
            });
        }
    });
}

run();
