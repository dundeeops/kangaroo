const { Transform } = require("stream");
const { StringDecoder } = require("string_decoder");

module.exports = class StringToLinesTransform extends Transform {
    constructor(options) {
        super(options);
        this._decoder = new StringDecoder(options && options.defaultEncoding);
        this.cacheStr = "";
    }

    sendData(str) {
        const data = this.cacheStr + str;
        const chunks = data.split("\n");
console.log(chunks);

        for (let i = 0; i < chunks.length - 1; i++) {
            if (chunks[i]) {
                this.push(chunks[i]);
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
        let data = this._decoder.end();
        this.sendData(data);
        if (this.cacheStr) {
            console.log("cahched", this.cacheStr);
            this.push(this.cacheStr);
        }
        console.log("closed", this.cacheStr);
        callback();
    }
}
