const { Writable } = require('stream');
const { StringDecoder } = require('string_decoder');

module.exports = class SendToProcessWritable extends Writable {
    constructor(options) {
        super(options);
        this._decoder = new StringDecoder(options && options.defaultEncoding);
        this._pool = options && options.pool;
    }

    _write(bytes, encoding, callback) {
        let chunk;

        if (encoding === 'buffer') {
            chunk = this._decoder.write(bytes);
        } else {
            chunk = bytes;
        }

        if (chunk) {
            this._pool.sendData(0, chunk);
        }

        callback();
    }

    _final(callback) {
        const chunk = this._decoder.end();

        if (chunk) {
            this._pool.sendData(0, chunk);
        }

        callback();
    }
}
