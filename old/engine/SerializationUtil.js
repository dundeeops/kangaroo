const safeStringify = require("fast-safe-stringify");
const crypto = require("crypto");

const {inject} = require("./InjectUtil.js");
const BaseDict = require("./BaseDict.js");

const separator = ":_:";

function cleanString(str) {
    return str.toString().replace(/(\n|\r)$/, "").trim();
}

function serializeData(data, _stringify = safeStringify) {
    return _stringify(data);
}

function makeMessage(data, _serializeData = serializeData) {
    return _serializeData(data) + BaseDict.ENDING;
}

function parseData(raw, _parse = JSON.parse, _cleanString = cleanString) {
    return _parse(_cleanString(raw));
}

function getInjectCrypto(...args) {
    return inject("crypto", crypto, args[args.length - 1]);
}

function getHash(...args) {
    const _crypto = getInjectCrypto(args);
    return _crypto
        .createHash("sha1")
        .update(
            args.join(separator),
        )
        .digest("base64");
}

function getId(...args) {
    const _crypto = getInjectCrypto(args);
    return _crypto.randomBytes(64).toString("hex");
}

module.exports = {
    cleanString,
    serializeData,
    makeMessage,
    parseData,
    getHash,
    getId,
}