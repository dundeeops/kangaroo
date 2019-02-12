const { pipeline, Transform, Writable, Readable } = require("stream");
const crypto = require("crypto");
const SendToProcessWritable = require("./SendToProcessWritable.js");
const StringToLinesTransform = require("./StringToLinesTransform.js");
const {
    deserializeData,
    serializeData,
} = require("./Serialization.js");

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
            this.getLinesStream(),
            this.getMapStream(),
            this.errorProcessing,
        )
    }

    setManagerStream(stage, stream) {
        const session = this.getId();
        return pipeline(
            stream,
            this.getLinesStream(),
            this.getSerializationStream(session, stage),
            this.getOutcomeStream(session, stage),
            this.errorProcessing,
        );
    }

    getLinesStream() {
        return new StringToLinesTransform();
    }

    getIncomeStream() {
        return this._server.getStream();
    }

    getHash(...args) {
        return crypto.createHash('sha1').update(args.join(separator)).digest('base64');
    }

    getId() {
        return this.getHash(Math.random().toString())
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

    getSerializationStream(session, stage, key) {
        return new Transform({
            transform(chunk, encoding, callback) {
                const data = chunk.toString();
                this.push(serializeData(session, stage, key, data));
                callback();
            },
        });
    }

    getOutcomeStream(session, stage, key) {
        return new SendToProcessWritable({
            mapReduceOrchestrator: this,
            serverName: this._server.getName(),
            session, stage, key,
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
                const { session, stage, key, data } = deserializeData(raw);
                const hash = mapReduceOrchestrator.getHash(session, stage, key);
                if (data) {
                    const stream = mapReduceOrchestrator.getStageKeyStreamOrCreate(hash, session, stage, key);
                    stream.push(raw);
                } else {
                    const stream = mapReduceOrchestrator.getStageKeyStream(hash);
                    if (stream) {
                        stream.push(null);
                        stream.destroy();
                    }
                }
                callback();
            },
            final(callback) {
                callback();
            }
        });
    }

    getStageKeyStream(hash) {
        return this._stageKeyStreamMap[hash] && this._stageKeyStreamMap[hash].stream;
    }

    getStageKeyStreamOrCreate(hash, session, stage, key) {
        if (!this._stageKeyStreamMap[hash]) {
            const mapReduceOrchestrator = this;
            const readStream = new Readable({
                autoDestroy: true,
                read() {},
                destroy(error, callback) {
                    mapReduceOrchestrator._stageKeyStreamMap[hash] = undefined;
                    delete mapReduceOrchestrator._stageKeyStreamMap[hash];
                    callback();
                },
            });
            
            this._stageKeyStreamMap[hash] = {
                stream: readStream,
                pipeline: pipeline(
                    readStream,
                    ...this.getMapStreams(session, stage, key),
                ),
            };
        }
        return this.getStageKeyStream(hash);
    }

    makeStream() {
        return new Readable({
            autoDestroy: true,
            read() {},
        });
    }

    getMapStreams(session, stage, key) {
        const mapper = this._mappers[stage];
        const stream = mapper(key);
        if (stream instanceof Readable) {
            // TODO: release used resources
            return [
                stream,
                this.getOutcomeStream(session, stage, key),
            ]
        } else {
            // TODO: release used resources
            return [
                stream,
            ];
        }
    }

    errorProcessing(err) {
        if (err) {
            console.error("Pipeline failed.", err);
        }
    }

    push(serverName, session, stage, key, data) {
        const nextServerName = this.getNextServer(serverName, stage, key);
        this.setNextServer(nextServerName, stage, key);
        const server = this._serverPool.getServer(nextServerName);
        const raw = serializeData(session, stage, key, data);
        server.sendData(raw + "\n");
    }
}
