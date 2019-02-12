const { Writable } = require("stream");
const { StringDecoder } = require("string_decoder");

module.exports = class SendToProcessWritable extends Writable {
    constructor(options) {
        super(options);
        this._decoder = new StringDecoder(options && options.defaultEncoding);
        this._mapReduceOrchestrator = options && options.mapReduceOrchestrator;
        this._stage = options && options.stage;
        this._key = options && options.key;
        this._serverName = options && options.serverName;
    }

    _write(bytes, encoding, callback) {
        let chunk;
        if (encoding === "buffer") {
            chunk = this._decoder.write(bytes);
        } else {
            chunk = bytes;
        }
        this._mapReduceOrchestrator.push(this._serverName, this._stage, this._key, chunk);
        callback();
    }

    _final(callback) {
        const chunk = this._decoder.end();
        this._mapReduceOrchestrator.push(this._serverName, this._stage, this._key, chunk);
        callback();
    }
}
