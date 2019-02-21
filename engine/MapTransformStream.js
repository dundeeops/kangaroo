const { Transform } = require("stream");
const {
    deserializeData,
    serializeData,
} = require("./SerializationUtil.js");

module.exports = class MapTransformStream extends Transform {
    constructor(options) {
        super(options);
    }

    parse(raw) {
        return deserializeData(raw);
    }

    send({ session, group, stage, key, data }) {
        this.push(serializeData({ session, group, stage, key, data }));
    }
}
