const safeStringify = require("fast-safe-stringify");

module.exports = {
    serializeData(stage, key, data) {
        return safeStringify({ stage, key, data });
    },

    deserializeData(raw) {
        return JSON.parse(raw.toString());
    }
}