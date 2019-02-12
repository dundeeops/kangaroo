const { Writable } = require("stream");
const net = require("net");

module.exports = class ServerConnection {
    constructor(options) {
        this.name = options.name;
        this.hostname = options.hostname;
        this.port = options.port;
        this._stream = this.makeStream();
    }

    async connect() {
        return await new Promise(async (r) => {
            this._socket = new net.Socket();
    
            this._socket.on("data", (data) => {
                this._stream.push(data.toString("utf8"));
            });
    
            this._socket.on("close", () => {
                console.log(`Disconnected with ${this.name}`);
                this._socket = null;
                r();
            });
    
            this._socket.on("connect", (data) => {
                console.log(`Connected with ${this.name}`);
                r();
            });

            this._socket.connect(
                this.port,
                this.hostname,
            );
        });
    }

    makeStream() {
        return new Writable({
            write(chunk, encoding, callback) {
                callback();
            },
            final(callback) {
                callback();
            }
        });
    }

    getStream() {
        return this._stream;
    }

    destroy() {
        if (this._socket) {
            this._socket.destroy();
        }
    }

    async sendData(data) {
        if (!this._socket) {
            await this.connect();
        }
        this._socket.write(data);
    }
}
