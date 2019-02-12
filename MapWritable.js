const { Writable } = require("stream");
const {
    deserializeData,
} = require("./Serialization.js");

module.exports = class MapWritable extends Writable {
    constructor(options) {
        super(options);
    }

    parse(raw) {
        return deserializeData(raw);
    }
}
