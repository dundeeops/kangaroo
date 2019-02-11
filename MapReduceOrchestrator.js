const { pipeline } = require("stream");
const crypto = require("crypto");
const SendToProcessWritable = require("./SendToProcessWritable.js");

const sha1 = crypto.createHash("sha1");
const separator = "::";

module.exports = class MapReduceOrchestrator {
    constructor(options) {
        this._serverPool = options.serverPool;
        this._server = options.server;
        this._initStage = options.initStage;
        this._config = options.config;
        this._mappers = {};
        this._serverStageKeyMap = {};
        this._serverStageKeyBubbling = [];
    }

    getHash(serverName, stage, key) {
        return sha1([serverName, stage, key].join(separator));
    }

    getNextServer(serverName, stage, key) {
        
    }

    getStream(options) {
        return new SendToProcessWritable({
            mapReduceOrchestrator: this,
            stage: (options && options.stage) || this._initStage,
            key: options && options.key,
            serverName: this._config.name,
        });
    }

    map(key, callbackStream) {
        this._mappers[key] = callbackStream;
    }

    runMap(stage, key, stream) {
        const mapper = this._mappers[stage];
        const [nextStage, nextKey, mapStream] = mapper(key);
        if (nextStage) {
            return pipeline(
                stream,
                mapStream,
                this.getStream({
                    stage: nextStage,
                    key: nextKey,
                }),
                this.errorProcessing,
            )
        } else {
            return pipeline(
                stream,
                mapStream,
                this.errorProcessing,
            )
        }
    }

    errorProcessing(err) {
        if (err) {
            console.error("Pipeline failed.", err);
        } else {
            console.log("Pipeline succeeded.");
        }
    }

    push(serverName, stage, key, data) {
        
    }
}
