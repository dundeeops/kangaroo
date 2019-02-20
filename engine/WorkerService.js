const {
    serializeData,
    getId,
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

        this._processingMap = {};
        this._storageMap = {};
            
        // TODO: Notify all key and send null to them
        // socket.write(serializeData({
        //     id,
        //     type: "finish",
        //     data: { session, stage, key },
        // }) + "\n");
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
            case "isProcessing":
                const hash = getHash(data.session, data.stage, data.key);
                socket.write(serializeData({
                    id,
                    type,
                    data: this._processingMap[hash] ? true : null,
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

    async makeMap(hash, session, _key, mapper) {
        const send = async (stage, key, data) => {
            this.setMapSessionKey(hash, stage, key);
            const serverName = await this.send(session, stage, key, data);
            this.addMapServerName(hash, stage, key, serverName);
        };
        const map = await mapper(_key, send);
        return map;
    }

    makeStorageMap(hash) {
        let resolveEOF = () => {};
        const promiseEOF = new Promise((r) => resolveEOF = r);
        this._storageMap[hash] = { map: null, keys: {}, session: 0, promiseEOF, resolveEOF };
    }

    setMapSessionKey(hash, stage, key) {
        const hashStageKey = getHash(stage, key);
        if (!this._storageMap[hash].keys[hashStageKey]) {
            this._storageMap[hash].keys[hashStageKey] = {
                stage, key, serverNames: [],
            };
        }
    }

    addMapServerName(hash, stage, key, serverName) {
        const hashStageKey = getHash(stage, key);
        if (this._storageMap[hash].keys[hashStageKey].serverNames.indexOf(serverName) === -1) {
            this._storageMap[hash].keys[hashStageKey].serverNames.push(serverName);
        }
    }

    forEachStorageMapSessionKey(hash, callback) {
        Object.keys(this._storageMap[hash].keys)
            .forEach((hashStageKey, index) => {
                const { stage, key, serverNames } = this._storageMap[hash].keys[hashStageKey];
                callback(stage, key, serverNames, index);
            });
    }

    increaseStorageMapCount(hash) {
        this._storageMap[hash].session++;
    }

    decreaseStorageMapCount(hash) {
        this._storageMap[hash].session--;
    }

    async waitStorageMapCount(hash) {
        await this._storageMap[hash].promiseEOF;
    }

    resolveStorageMapCount(hash) {
        if (this._storageMap[hash].session === 0) {
            this._storageMap[hash].resolveEOF();
        }
    }

    destroyStorageMap(hash) {
        delete this._storageMap[hash];
    }

    getMap(hash) {
       return this._storageMap[hash] && this._storageMap[hash].map;
    }

    checkStorageMap(hash) {
        let map = this._storageMap[hash];

        if (!map) {
            this.makeStorageMap(hash);
        }
    }

    async getStorageMap(hash, session, stage, key) {
        const storageMap = this._storageMap[hash];

        if (!storageMap.map) {
            const mapper = this.getMapper(stage);
            storageMap.map = await this.makeMap(hash, session, key, mapper);
        }

        return storageMap;
    }

    async onData(socket, obj) {
        const {key} = obj;
        if (key == null) {
            this.onDataMap(socket, obj);
        } else {
            this.onDataReduce(socket, obj);
        }
    }

    async onDataReduce(socket, { session, stage, key, data }) {
        const hash = getHash(session, stage, key);

        if (!this._storageMap[hash] && data == null) {
            return;
        }
        
        this.checkStorageMap(hash);

        if (data != null) {
            this.increaseStorageMapCount(hash);
        }

        const storageMap = await this.getStorageMap(hash, session, stage, key);
        await storageMap.map({ stage, key, data, eof: !data });
        
        if (data != null) {
            this.decreaseStorageMapCount(hash);
            this.resolveStorageMapCount(hash);
        }

        if (data == null) {
            await this.waitStorageMapCount(hash);
            console.log("Notify Map Finished", session, stage, key, !!data);
            this.forEachStorageMapSessionKey(hash, (stage, key, serverNames) => {
                serverNames.forEach((serverName) => {
                    this.sendToServer(serverName, session, stage, key, null);
                });
            });

            this.destroyStorageMap(hash);
        }
    }

    async onDataMap(socket, { session, stage, data }) {
        const hash = getHash(session, stage, getId());
        const hashMap = getHash(session, stage, null);
        
        this._processingMap[hashMap] = (this._processingMap[hashMap] || 0) + 1;

        if (!this._storageMap[hash] && data == null) {
            console.log("Notify All Finished", session, stage);
            
            await this.notify("finishProcessing", {
                session, stage, key: null,
            });

            return;
        }

        this.checkStorageMap(hash);

        if (data != null) {
            const storageMap = await this.getStorageMap(hash, session, stage, null);
            await storageMap.map({ stage, key: null, data, eof: false });
            await storageMap.map({ stage, key: null, data: null, eof: true });
        }

        this._processingMap[hashMap] = (this._processingMap[hashMap] || 0) - 1;

        const isReady = await this.ask("isProcessing", {
            session, stage, key: null,
        });
console.log("isReady", isReady);

        if (isReady) {
            this.forEachStorageMapSessionKey(hash, (stage, key, serverNames) => {
                // serverNames.forEach((serverName) => {
                //     this.sendToServer(serverName, session, stage, key, null);
                // });
            });
        }

        this.destroyStorageMap(hash);
    }
}
