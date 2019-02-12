const { Readable } = require("stream");
const { ServerFactory } = require("./ServerFactory.js");

module.exports = class ServerEntry {
    constructor(options) {
        this.name = options.name;
        this.hostname = options.hostname;
        this.port = options.port;
        this._stream = this.makeStream();
    }

    async init() {
        await this.runServer();
    }

    async runServer() {
        const server = this;
        return await new Promise(async (r) => {
            this._server = ServerFactory({
                port: this.port,
                hostname: this.hostname,
                prepare: (socket) => {
                    socket.on("data", function(data) {
                        server._stream.push(data.toString("utf8"));
                    });
                },
                onConnect: () => r(),
            });

            this._server.on("end", () => {
                r();
            });
        });
    }

    makeStream() {
        return new Readable({
            read() {},
            final(callback) {
                callback();
            }
        });
    }

    getStream() {
        return this._stream;
    }
}
