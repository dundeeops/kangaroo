const { Writable } = require("stream");
const { StringDecoder } = require("string_decoder");
const {
    deserializeData,
} = require("./SerializationUtil.js");

module.exports = class SendWritableStream extends Writable {
    constructor(options) {
        super(options);
        this._decoder = new StringDecoder(options.defaultEncoding);
        this._send = options.send;
        this._onFinish = options.onFinish;
        this._session = options.session;
        this._group = options.group;
        this._stage = options.stage;
        this._key = options.key;
    }

    send(raw) {
        const { stage, key, data } = deserializeData(raw);
        // console.log("COMPARE", this._stage, this._key, stage, key);
        
        this._send(this._session, this._group, stage, key, data);
    }

    // sendFinal() {
    //     this._send(this._session, this._stage, this._key, null);
    // }

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

        // this.sendFinal();
        this._onFinish();

        callback();
    }
}
