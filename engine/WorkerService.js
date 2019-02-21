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
        // this._waitingNullMap = {};
        // this._waitingNullResolveMap = {};
        // this._storageMap = {};
            
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
                socket.write(serializeData({
                    id,
                    type,
                    data: this._processingMap[data.group]
                        && this._processingMap[data.group].processes
                            ? true
                            : null,
                }) + "\n");
                break;
            case "nullAchived":
                await this.startUnlessTimeout(async () => {
                    if (this._processingMap[data.group]) {
                        const isReady = !this._processingMap[data.group].processes
                            && !await this.ask("isProcessing", { group: data.group });

                        if (isReady) {
                            this.forEachStorageMaps(data.group, (map) => {
                                map.onFinish();
                            });

                            this.forEachUsedGroups(data.group, (nextGroup) => {
                                this.notify("nullAchived", {
                                    group: nextGroup,
                                });
                            });

                            delete this._processingMap[data.group];
                        }

                        return !isReady;
                    } else {
                        return false;
                    }
                }, 100);
                break;
        }
    }

    getMappers() {
        return Object.keys(this._mappers);
    }

    setMap(key, map) {
        this._mappers[key] = map;
    }

    async start() {
        await this._server.start();
    }

    getMapper(stage) {
        return this._mappers[stage];
    }

    getSendWrap(group, session) {
        return async (stage, key, data) => {
            const nextGroup = getHash(group, stage);
            this.setUsedGroup(group, nextGroup);
            await this.send(session, nextGroup, stage, key, data);
        };
    }

    async makeMap(group, session, key, mapper) {
        const send = this.getSendWrap(group, session);
        const map = await mapper(key, send);
        if (Array.isArray(map)) {
            return {
                onData: map[0],
                onFinish: map[1],
            };
        } else {
            return {
                onData: map,
                onFinish: () => {},
            };
        }
    }

    makeStorageMap(group, hash) {
        this._processingMap[group].storageMap[hash] = { map: null };
    }

    setUsedGroup(group, nextGroup) {
        if (this._processingMap[group].usedGroups.indexOf(nextGroup) === -1) {
            this._processingMap[group].usedGroups.push(nextGroup);
        }
    }

    forEachStorageMaps(group, callback) {
        Object.keys(this._processingMap[group].storageMap)
            .forEach((hash, index) => {
                callback(this._processingMap[group].storageMap[hash].map, hash, index);
            });
    }

    forEachUsedGroups(group, callback) {
        this._processingMap[group].usedGroups
            .forEach((name, index) => {
                callback(name, index);
            });
    }

    destroyStorageMap(group, hash) {
        delete this._processingMap[group].storageMap[hash];
    }

    getMap(group, hash) {
        return this._processingMap[group].storageMap[hash]
            && this._processingMap[group].storageMap[hash].map;
    }

    checkStorageMap(group, hash) {
        if (!this._processingMap[group].storageMap[hash]) {
            this.makeStorageMap(group, hash);
        }
    }

    async getStorageMap(group, hash, session, stage, key) {
        const storageMap = this._processingMap[group].storageMap[hash];

        if (!storageMap.map) {
            const mapper = this.getMapper(stage);
            storageMap.map = await this.makeMap(group, session, key, mapper);
        }

        return storageMap;
    }

    checkProcessingMap(group) {
        if (!this._processingMap[group]) {
            this._processingMap[group] = {
                processes: 0,
                storageMap: {},
                usedGroups: [],
            };
        }
    }

    async onData(_socket, { session, group, stage, key, data }) {
        const hash = getHash(session, stage, key || getId());

        this.checkProcessingMap(group);
        
        this._processingMap[group].processes++;

        this.checkStorageMap(group, hash);

        const storageMap = await this.getStorageMap(group, hash, session, stage, key);
        await storageMap.map.onData({ stage, key, data, eof: !data });

        if (!key) {
            this.destroyStorageMap(group, hash);
        }

        this._processingMap[group].processes--;
    }

    async startUnlessTimeout(callback, timeout) {
        let func;
        func = async () => {
            const result = await callback();
            if (result) {
                setTimeout(func, timeout);
            }
        }
        await func();
    }
}
