const { Transform } = require("stream");
const {
    deserializeData,
    serializeData,
} = require("./Serialization.js");

module.exports = class MapTransform extends Transform {
    constructor(options) {
        super(options);
    }

    parse(raw) {
        return deserializeData(raw);
    }

    send({ session, stage, key, data }) {
        this.push(serializeData({ session, stage, key, data }));
    }
}
