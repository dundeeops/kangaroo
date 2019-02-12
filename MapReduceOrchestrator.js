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
        this._mappers = {};
        this._serverStageMap = {};
        this._stageKeyMap = {};
        this._stageKeyStreamMap = {};
    }

    async runWorker() {
        this._server.checkRunning();

        return pipeline(
            this.getIncomeStream(),
            new StringToLinesTransform(),
            this.getMapStream(),
            this.errorProcessing,
        )
    }

    setManagerStream(stream, stage, key) {
        return pipeline(
            stream,
            new StringToLinesTransform(),
            this.getOutcomeStream(stage, key),
            this.errorProcessing,
        );
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
            const aServerHash = this.getHash(a.getName(), stage);
            const aKeyCount = getServerStageKeyCount(aServerHash);
            const bServerHash = this.getHash(b.getName(), stage);
            const bKeyCount = getServerStageKeyCount(bServerHash);
            if (aKeyCount === bKeyCount) {
                if (a.getName() === serverName) {
                    return 1;
                } else if (b.getName() === serverName) {
                    return -1;
                }
            } else {
                return aKeyCount < bKeyCount ? 1 : -1;
            }
        }).map((server) => server.getName());
    }

    getNextServer(serverName, stage, key) {
        const stageKeyHash = this.getHash(stage, key);
        if (
            key != null && this._stageKeyMap[stageKeyHash]
        ) {
            return this._stageKeyMap[stageKeyHash];
        } else {
            const sorted = this.getServerStageKeySorted(serverName, stage);
            return sorted[0] || serverName;
        }
    }

    setNextServer(serverName, stage, key) {
        if (key != null) {
            const stageKeyHash = this.getHash(stage, key);
            this._stageKeyMap[stageKeyHash] = serverName;
        }
        const serverStageHash = this.getHash(serverName, stage);
        if (!this._serverStageMap[serverStageHash]) {
            this._serverStageMap[serverStageHash] = 1;
        } else {
            this._serverStageMap[serverStageHash]++;
        }
    }

    getOutcomeStream(stage, key) {
        return new SendToProcessWritable({
            mapReduceOrchestrator: this,
            stage: stage || this._initStage,
            key,
            serverName: this._server.getName(),
        });
    }

    map(key, callbackStream) {
        this._mappers[key] = callbackStream;
    }

    getMapStream() {
        const mapReduceOrchestrator = this;
        return new Writable({
            write(chunk, encoding, callback) {
                const raw = chunk.toString();
                const { stage, key, data } = mapReduceOrchestrator.deserializeData(raw);
                const stream = mapReduceOrchestrator.getStageKeyStream(stage, key);
                stream.push(data);
                callback();
            },
        });
    }

    getStageKeyStream(stage, key) {
        const mapReduceOrchestrator = this;
        const hash = this.getHash(stage, key);
        if (!this._stageKeyStreamMap[hash]) {
            const stream = new Readable({
                read() {},
                final(callback) {
                    delete mapReduceOrchestrator._stageKeyStreamMap[hash];
                    callback();
                }
            });
            this._stageKeyStreamMap[hash] = {
                stream,
                pipeline: pipeline(
                    stream,
                    ...this.getMapStreams(stage, key),
                ),
            };
        }
        return this._stageKeyStreamMap[hash].stream;
    }

    makeStream() {
        return new Readable({
            read() {},
        });
    }

    getMapStreams(stage, key) {
        const mapper = this._mappers[stage];
        const [nextStage, nextKey, mapStream] = mapper(key);
        if (nextStage) {
            return [
                mapStream,
                this.getOutcomeStream(nextStage, nextKey),
            ];
        } else {
            return [mapStream];
        }
    }

    errorProcessing(err) {
        if (err) {
            console.error("Pipeline failed.", err);
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
