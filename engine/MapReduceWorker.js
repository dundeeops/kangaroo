const { pipeline, Writable, Readable } = require("stream");
const {
    deserializeData,
} = require("./Serialization.js");
const MapReduceBase = require("./MapReduceOrchestrator.js");
const ServerEntry = require("./ServerEntry.js");

module.exports = class MapReduceManagerWorker extends MapReduceBase {

    constructor(options) {
        super(options);
        this._server = new ServerEntry({
            name: options.server.name,
            hostname: options.server.hostname,
            port: options.server.port,
            getMappers: () => this.getMappers(),
        });
        this._mappers = {};
        this._stageKeyStreamMap = {};
    }

    getMappers() {
        return Object.keys(this._mappers);
    }

    setMap(key, callbackStream) {
        this._mappers[key] = callbackStream;
    }

    async run() {
        this._server.checkRunning();

        // TODO: error processing
        return pipeline(
            this.getIncomeStream(),
            this.getLinesStream(),
            this.getMapStream(),
            this.errorProcessing,
        )
    }

    getIncomeStream() {
        return this._server.getStream();
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
}
