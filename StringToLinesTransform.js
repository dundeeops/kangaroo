const { Transform } = require("stream");
const { StringDecoder } = require("string_decoder");

module.exports = class StringToLinesTransform extends Transform {
    constructor(options) {
        super(options);
        this._decoder = new StringDecoder(options && options.defaultEncoding);
        this.cacheStr = "";
    }

    cleanString(str) {
        return str.replace(/(\n|\r)$/, '').trim();
    }

    sendData(str) {
        const data = this.cacheStr + str;
        const chunks = data.split("\n");

        for (let i = 0; i < chunks.length - 1; i++) {
            if (chunks[i]) {
                this.push(this.cleanString(chunks[i]));
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
        const str = this.cleanString(this.cacheStr);
        if (str) {
            this.push(str);
        }
        callback();
    }
}
