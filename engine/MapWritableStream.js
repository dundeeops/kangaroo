const { Writable } = require("stream");
const {
    deserializeData,
} = require("./SerializationUtil.js");

module.exports = class MapWritableStream extends Writable {
    constructor(options) {
        super(options);
    }

    parse(raw) {
        return deserializeData(raw);
    }
}
