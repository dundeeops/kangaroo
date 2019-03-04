const safeStringify = require("fast-safe-stringify");
const crypto = require("crypto");

const {inject} = require("./InjectUtil.js");

const separator = ":_:";

function cleanString(str) {
    return str.toString().replace(/(\n|\r)$/, "").trim();
}

function serializeData(data, _stringify = safeStringify) {
    return _stringify(data);
}

function parseData(raw, _parse = JSON.parse, _cleanString = cleanString) {
    return _parse(_cleanString(raw));
}

function getServerName(hostname, port) {
    return `${hostname}:${port}`;
}

function getHash(...args) {
    const _crypto = inject("crypto", crypto, args[args.length - 1]);
    return _crypto
        .createHash("sha1")
        .update(
            args.join(separator),
        )
        .digest("base64");
}

function getId(_getHash = getHash, _random = Math.random) {
    return getHash(_random().toString())
}

module.exports = {
    cleanString,
    serializeData,
    parseData: parseData,
    getServerName,
    getHash,
    getId,
}