const safeStringify = require("fast-safe-stringify");

module.exports = {
    serializeData(data) {
        return safeStringify(data);
    },

    deserializeData(raw) {
        return JSON.parse(raw.toString());
    }
}