const { pipeline, Writable, Readable } = require("stream");
const crypto = require("crypto");
const safeStringify = require("fast-safe-stringify");
const SendToProcessWritable = require("./SendToProcessWritable.js");
const StringToLinesTransform = require("./StringToLinesTransform.js");

const separator = ":_:";

module.exports = class MapReduceOrchestrator {
    constructor(options) {
        this._serverPool = options.serverPool;
        this._server = options.server;
        this._initStage = options.initStage;
        this._name = options.name;
        this._mappers = {};
        this._serverStageMap = {};
        this._stageKeyMap = {};
        this._stageKeyStreamMap = {};
    }

    async init() {
        await this._server.init();

        return pipeline(
            this.getIncomeStream(),
            new StringToLinesTransform(),
            this.getStream(),
            this.errorProcessing,
        )
    }

    getIncomeStream() {
        return this._server.getStream();
    }

    getHash(...args) {
        return crypto.createHash('sha1').update(args.join(separator)).digest('base64');
    }

    getServerStageKeyCount(serverStageHash) {
        return this._serverStageMap[serverStageHash] || 0;
    }

    getServerStageKeySorted(serverName, stage) {
        return this._serverPool.getServers().sort((a, b) => {
            const aServerHash = this.getHash(a.name, stage);
            const aKeyCount = getServerStageKeyCount(aServerHash);
            const bServerHash = this.getHash(b.name, stage);
            const bKeyCount = getServerStageKeyCount(bServerHash);
            if (aKeyCount === bKeyCount) {
                if (a.name === serverName) {
                    return 1;
                } else if (b.name === serverName) {
                    return -1;
                }
            } else {
                return aKeyCount < bKeyCount ? 1 : -1;
            }
        }).map((server) => server.name);
    }

    getNextServer(serverName, stage, key) {
        const stageKeyHash = this.getHash(stage, key);
        if (
            this._stageKeyMap[stageKeyHash]
        ) {
            return this._stageKeyMap[stageKeyHash];
        } else {
            const sorted = this.getServerStageKeySorted(serverName, stage);
            return sorted[0] || serverName;
        }
    }

    setNextServer(serverName, stage, key) {
        const stageKeyHash = this.getHash(stage, key);
        const serverStageHash = this.getHash(serverName, stage);
        this._stageKeyMap[stageKeyHash] = serverName;
        if (!this._serverStageMap[serverStageHash]) {
            this._serverStageMap[serverStageHash] = 1;
        } else {
            this._serverStageMap[serverStageHash]++;
        }
    }

    getOutcomeStream(options) {
        return new SendToProcessWritable({
            mapReduceOrchestrator: this,
            stage: (options && options.stage) || this._initStage,
            key: options && options.key,
            serverName: this._name,
        });
    }

    map(key, callbackStream) {
        this._mappers[key] = callbackStream;
    }

    getStream() {
        const mapReduceOrchestrator = this;
        return new Writable({
            write(chunk, encoding, callback) {
                const raw = chunk.toString();
                const { stage, key, data } = mapReduceOrchestrator.deserializeData(raw);
                const stream = mapReduceOrchestrator.getStageKeyStream(stage, key);
                stream.push(data || null);
                callback();
            },

            final(callback) {
                callback();
            }
        });
    }

    getStageKeyStream(stage, key) {
        const mapReduceOrchestrator = this;
        const hash = this.getHash(stage, key);
        if (!this._stageKeyStreamMap[hash]) {
            this._stageKeyStreamMap[hash] = new Readable({
                final(callback) {
                    delete mapReduceOrchestrator._stageKeyStreamMap[hash];
                    callback();
                }
            });
            pipeline(
                this._stageKeyStreamMap[hash],
                ...this.mapStream(stage, key),
            )
        }
        return this._stageKeyStreamMap[hash];
    }

    makeStream() {
        return new Readable({});
    }

    mapStream(stage, key) {
        const mapper = this._mappers[stage];
        const [nextStage, nextKey, mapStream] = mapper(key);
        if (nextStage) {
            return [
                mapStream,
                this.getOutcomeStream({
                    stage: nextStage,
                    key: nextKey,
                }),
            ];
        } else {
            return [
                mapStream,
            ];
        }
    }

    errorProcessing(err) {
        if (err) {
            console.error("Pipeline failed.", err);
        } else {
            console.log("Pipeline succeeded.");
        }
    }

    serializeData(stage, key, data) {
        return safeStringify({ stage, key, data });
    }

    deserializeData(raw) {
        return JSON.parse(raw);
    }

    push(serverName, stage, key, data) {
        const nextServerName = this.getNextServer(serverName, stage, key);
        this.setNextServer(nextServerName, stage, key);
        const server = this._serverPool.getServer(nextServerName);
        const raw = this.serializeData(stage, key, data);
        server.sendData(raw + "\n");
    }
}
