const safeStringify = require("fast-safe-stringify");
const crypto = require("crypto");

const separator = ":_:";

function serializeData(data) {
    return safeStringify(data);
}

function deserializeData(raw) {
    return JSON.parse(raw.toString());
}

function getServerName(hostname, port) {
    return `${hostname}:${port}`;
}

function getHash(...args) {
    return crypto.createHash('sha1').update(args.join(separator)).digest('base64');
}

function getId() {
    return getHash(Math.random().toString())
}

module.exports = {
    serializeData,
    deserializeData,
    getServerName,
    getHash,
    getId,
}