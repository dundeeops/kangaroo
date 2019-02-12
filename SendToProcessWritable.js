const { Writable } = require("stream");
const { StringDecoder } = require("string_decoder");
const {
    deserializeData,
} = require("./Serialization.js");

module.exports = class SendToProcessWritable extends Writable {
    constructor(options) {
        super(options);
        this._decoder = new StringDecoder(options && options.defaultEncoding);
        this._mapReduceOrchestrator = options && options.mapReduceOrchestrator;
        this._serverName = options && options.serverName;
        this._session = options && options.session;
        this._stage = options && options.stage;
        this._key = options && options.key;
    }

    send(raw) {
        const { stage, key, data } = deserializeData(raw);
        this._mapReduceOrchestrator.push(this._serverName, this._session, stage, key, data);
    }

    sendFinal() {
        this._mapReduceOrchestrator.push(this._serverName, this._session, this._stage, this._key, null);
    }

    _write(bytes, encoding, callback) {
        let chunk;
        if (encoding === "buffer") {
            chunk = this._decoder.write(bytes);
        } else {
            chunk = bytes;
        }

        if (chunk) {
            this.send(chunk);
        }

        callback();
    }

    _final(callback) {
        const chunk = this._decoder.end();

        if (chunk) {
            this.send(chunk);
        }

        this.sendFinal();

        callback();
    }
}
