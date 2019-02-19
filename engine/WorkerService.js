const { pipeline, Writable, Readable } = require("stream");
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
        // TODO: Make error processing
        // TODO: Make accumulator
        this._server = options.serverInstance || new WorkerServer({
            hostname: options.server.hostname,
            port: options.server.port,
            getMappers: () => this.getMappers(),
            onAnswer: async (...args) => await this.onAsk(...args),
            onData: async (...args) => await this.onData(...args),
        });
        this._mappers = {};
        this._stageKeyStreamMap = {};

        this._streamMap = {};
        this._queue = [];
    }

    async onAsk(socket, id, type, data) {
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

    async start() {
        await this._server.start();
    }

    getIncomeStream() {
        return this._server.getStream();
    }

    getMapper(stage) {
        const mapper = this._mappers[stage];
        return mapper;
    }

    async makeMap(session, _stage, _key, mapper) {
        const send = async (stage, key, data) => {
            this.setMapSessionKey(session, _stage, _key, {stage, key});
            await this.send(session, stage, key, data);
        };
        const map = await mapper(_key, send);
        return map;
    }

    setMap(hash, map) {
        this._streamMap[hash] = { map, keys: {} };
    }

    setMapSessionKey(session, stage, key, data) {
        const hash = getHash(session, stage, key);
        const hashStageKey = getHash(data.stage, data.key);
        if (!this._streamMap[hash].keys[hashStageKey]) {
            this._streamMap[hash].keys[hashStageKey] = {
                stage: data.stage, key: data.key,
            };
        }
    }

    forEachMapSessionKey(session, stage, key, callback) {
        const hash = getHash(session, stage, key);
        if (this._streamMap[hash]
            && this._streamMap[hash].keys[hashStageKey]) {
            Object.keys(this._streamMap[hash].keys).forEach(({stage, key}, index) => callback(stage, key, index))
        }
    }

    // increaseMapSessionKey(session, stage, key) {
    //     const hash = getHash(session, stage, key);
    //     const hashStageKey = getHash(session, stage, key);
    //     if (!this._streamMap[hash].session[hashStageKey]) {
    //         this._streamMap[hash].session[hashStageKey] = 0;
    //     }
    //     this._streamMap[hash].session[hashStageKey]++;
    // }

    // decreaseMapSessionKey(session, stage, key) {
    //     const hash = getHash(session, stage, key);
    //     const hashStageKey = getHash(session, stage, key);
    //     if (this._streamMap[hash].session[hashStageKey]) {
    //         this._streamMap[hash].session[hashStageKey]--;
    //     }
    // }

    unsetMap(session, stage, key) {
        const hash = getHash(session, stage, key);
        delete this._streamMap[hash];
    }

    getMap(hash) {
       return this._streamMap[hash] && this._streamMap[hash].map;
    }

    async getStorageMap(session, stage, key) {
        const hash = getHash(session, stage, key);
        let map = this._streamMap[hash];

        if (!map) {
            const mapper = this.getMapper(stage);
            map = await this.makeMap(session, stage, key, mapper);
            this.setMap(hash, map);
        }

        return map;
    }

    async onData(socket, obj) {
        this._queue.push({socket, obj});
        await this._queuePromise;
        await this.emptyQueue();
    }

    async emptyQueue() {
        const {socket, obj} = this._queue.shift();
        this._queuePromise = new Promise(async (r) => {
            await this.onDataProcess(socket, obj);
            r();
        });
        await this._queuePromise;
    }

    async onDataProcess(socket, { session, stage, key, data }) {
        const hash = getHash(session, stage, key);

        if (data != null) {
            console.log("BEFORE OPEN MAP", stage, key);
            const map = await this.getStorageMap(session, stage, key);
            await map({ stage, key, data });
            console.log("OPEN MAP", stage, key, this._streamMap[hash]);
        } else {
            console.log("CLOSE MAP", stage, key, this._streamMap[hash]);

            this.forEachMapSessionKey(session, stage, key, (stage, key, index) => {
                console.log("GET", stage, key, index);
            })
            
            // socket.write(serializeData({
            //     id,
            //     type: "finish",
            //     data: { session, stage, key },
            // }) + "\n");

            // TODO: Notify all key and send null to them

            this.unsetMap(session, stage, key);
        }
    }

    // TODO: Remove all below

    getMapStream() {
        const _service = this;
        return new Writable({
            async write(chunk, encoding, callback) {
                const raw = chunk.toString();
                const { session, stage, key, data } = deserializeData(raw);

                if (data != null) {
                    const map = await _service.getStorageMap(session, stage, key);
                    await map({ stage, key, data });
                } else {
                    _service.unsetMap(session, stage, key);
                }

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
