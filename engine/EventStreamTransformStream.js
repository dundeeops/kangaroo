const { Transform } = require("stream");
const { StringDecoder } = require("string_decoder");
const {
    cleanString,
} = require("./SerializationUtil.js");

module.exports = class EventStreamTransformStream extends Transform {
    constructor(options) {
        super(options);
        this._decoder = new StringDecoder(options && options.defaultEncoding);
        this.cacheStr = "";
    }

    sendData(str) {
        const data = this.cacheStr + str;
        const chunks = data.split("\n");

        for (let i = 0; i < chunks.length - 1; i++) {
            if (chunks[i]) {
                this.push(cleanString(chunks[i]));
            }
        }

        if (chunks[chunks.length - 1]) {
            this.cacheStr = chunks[chunks.length - 1];
        } else {
            this.cacheStr = "";
        }
    }

    _transform(chunk, encoding, callback) {
        let data;

        if (encoding === "buffer") {
            data = this._decoder.write(chunk);
        } else {
            data = chunk;
        }

        this.sendData(data);

        callback();
    }

    _final(callback) {
        const data = this._decoder.end();
        this.sendData(data);
        const str = cleanString(this.cacheStr);
        if (str) {
            this.push(str);
        }
        callback();
    }
}
