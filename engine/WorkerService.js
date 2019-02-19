const { pipeline, Writable, Readable, Transform } = require("stream");
const {
    serializeData,
    deserializeData,
    getHash,
} = require("./SerializationUtil.js");
const OrchestratorServicePrototype = require("./OrchestratorServicePrototype.js");
const WorkerServer = require("./WorkerServer.js");

module.exports = class WorkerService extends OrchestratorServicePrototype {

    constructor(options) {
        super(options);
        this._server = options.serverInstance || new WorkerServer({
            hostname: options.server.hostname,
            port: options.server.port,
            getMappers: () => this.getMappers(),
            onAnswer: this.answer.bind(this),
        });
        this._mappers = {};
        this._stageKeyStreamMap = {};

        this._streamMap = {};
    }

    answer(socket, id, type, data) {
        switch (type) {
            case "getSessionStageKeyServer":
                const serverName = this.getSessionStageKeyServer(data.session, data.stage, data.key);                
                socket.write(serializeData({
                    id,
                    type,
                    data: serverName,
                }) + "\n");
                break;
        }
    }

    getMappers() {
        return Object.keys(this._mappers);
    }

    setStream(key, callbackStream) {
        this._mappers[key] = callbackStream;
    }

    async run() {
        await this._server.runAndWait();

        // TODO: Make error processing
        // TODO: Make accumulator
        return this.getIncomeStream()
            .pipe(this.getMapStream());
    }

    getIncomeStream() {
        return this._server.getStream();
    }

    getMapper(stage) {
        const mapper = this._mappers[stage];
        return mapper;
    }

    async makeMap(session, key, mapper) {
        const send = async (stage, key, data) => await this.send(session, stage, key, data);
        const map = await mapper(key, send);
        return map;
    }

    async setMap(hash, map) {
        this._streamMap[hash] = map;
    }

    async getMap(session, stage, key) {
        const hash = getHash(session, stage, key);
        let map = this._streamMap[hash];

        if (!map) {
            const mapper = this.getMapper(stage);
            map = await this.makeMap(session, key, mapper);
            this.setMap(hash, map);
        }

        return map;
    }

    getMapStream() {
        const _service = this;
        return new Writable({
            async write(chunk, encoding, callback) {
                const raw = chunk.toString();
                const { session, stage, key, data } = deserializeData(raw);
                const map = await _service.getMap(session, stage, key);

                await map({ stage, key, data });

                callback();
            },
            final(callback) {
                callback();
            }
        });
    }

    _getMapStream() {
        const _service = this;
        return new Writable({
            write(chunk, encoding, callback) {
                const raw = chunk.toString();
                
                const { session, stage, key, data } = deserializeData(raw);
                const hash = getHash(session, stage, key);
                if (data) {
                    const stream = _service.getStageKeyStreamOrCreate(hash, session, stage, key);
                    stream.push(raw);
                } else {
                    const stream = _service.getStageKeyStream(hash);
                    console.log(!!stream, stage, key, data);
                    
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
            const _service = this;
            const readStream = new Readable({
                autoDestroy: true,
                read() {},
                destroy(error, callback) {
                    _service._stageKeyStreamMap[hash] = undefined;
                    delete _service._stageKeyStreamMap[hash];
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
