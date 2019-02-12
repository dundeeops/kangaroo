const safeStringify = require("fast-safe-stringify");

module.exports = {
    serializeData(session, stage, key, data) {
        return safeStringify({ session, stage, key, data });
    },

    deserializeData(raw) {
        return JSON.parse(raw.toString());
    }
}