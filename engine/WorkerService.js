const {
    serializeData,
    getId,
    getHash,
} = require("./SerializationUtil.js");
const {
    startUnlessTimeout,
} = require("./PromisifyUtil");
const OrchestratorServicePrototype = require("./OrchestratorServicePrototype.js");
const WorkerServer = require("./WorkerServer.js");
const Dict = require("./AskDict.js");

const defaultOptions = {
    inject: {
        _startUnlessTimeout: startUnlessTimeout,
    },
};

// TODO: Make error processing
// TODO: Make accumulator
module.exports = class WorkerService extends OrchestratorServicePrototype {

    constructor(_options) {
        const options = {
            ...defaultOptions,
            ..._options,
        };
        super(options);

        this.initInjections(options);

        this.initWorker(options);
        this.initServer(options);
        this.initOnAskMap();
    }

    initInjections(options) {
        this._startUnlessTimeout = options.inject._startUnlessTimeout;
    }

    initWorker() {
        this._mappers = new Map();
        this._processingMap = new Map();
    }

    initServer(options, _WorkerServer = WorkerServer) {
        this._server = new _WorkerServer({
            hostname: options.hostname,
            port: options.port,
            getMappers: this.getMappers.bind(this),
            onAsk: this.onAsk.bind(this),
            onData: this.onData.bind(this),
        });
    }

    initOnAskMap() {
        this.onAskMap = {
            [Dict.GET_SESSION_STAGE_KEY_SERVER]: this.onAskGetSessionStageKeyServer.bind(this),
            [Dict.IS_PROCESSING]: this.onAskIsProcessing.bind(this),
            [Dict.NULL_ACHIVED]: this.onAskNullAchived.bind(this),
        };
    }
    
    onAskGetSessionStageKeyServer(socket, id, type, data, _serializeData = serializeData) {
        const serverName = this.getSessionStageKeyServer(data.session, data.stage, data.key);
        socket.write(_serializeData({
            id,
            type,
            data: serverName,
        }) + Dict.ENDING);
    }
    
    onAskIsProcessing(socket, id, type, data, _serializeData = serializeData) {
        socket.write(_serializeData({
            id,
            type,
            data: this._processingMap.get(data.group)
                && this._processingMap.get(data.group).processes
                    ? true
                    : null,
        }) + Dict.ENDING);
    }
    
    async onAskNullAchived(socket, id, type, data) {
        await this._startUnlessTimeout(async () => {
            if (this._processingMap.get(data.group)) {
                const isReady = !this._processingMap.get(data.group).processes
                    && !await this.ask(Dict.IS_PROCESSING, { group: data.group });

                if (isReady) {
                    this.forEachStorageMaps(data.group, (map) => {
                        map.onFinish();
                    });

                    this.forEachUsedGroups(data.group, (nextGroup) => {
                        this.notify(Dict.NULL_ACHIVED, {
                            group: nextGroup,
                        });
                    });

                    this._processingMap.delete(data.group);
                }

                return !isReady;
            } else {
                return false;
            }
        }, 100);
    }

    // TODO: Extract ask names
    async onAsk(socket, id, type, data) {
        await this.onAskMap[type](socket, id, type, data);
    }

    async start() {
        await this._server.start();
    }

    getMappers() {
        return Array.from(this._mappers.keys());
    }

    setMapper(stage, map) {
        this._mappers.set(stage, map);
    }

    getMapper(stage) {
        return this._mappers.get(stage);
    }

    getSendWrap(group, session, _getHash = getHash) {
        return async (stage, key, data) => {
            const nextGroup = _getHash(group, stage);
            this.setUsedGroup(group, nextGroup);
            await this.send(session, nextGroup, stage, key, data);
        };
    }

    parseMapperResult(mapResult) {
        if (Array.isArray(mapResult)) {
            return [mapResult[0], mapResult[1]];
        } else {
            return [mapResult, () => {}];
        }
    }

    makeMapBody(onData, onFinish) {
        return { onData, onFinish };
    }

    async makeMap(group, session, key, mapper) {
        const sendWrap = this.getSendWrap(group, session);
        const mapResult = await mapper(key, sendWrap);
        const mapCouple = this.parseMapperResult(mapResult);
        return this.makeMapBody(mapCouple[0], mapCouple[1]);
    }

    makeStorageMap() {
        return {
            map: null,
        };
    }

    setUsedGroup(group, nextGroup) {
        if (this._processingMap.get(group).usedGroups.indexOf(nextGroup) === -1) {
            this._processingMap.get(group).usedGroups.push(nextGroup);
        }
    }

    forEachStorageMaps(group, callback) {
        Object.keys(this._processingMap.get(group).storageMap)
            .forEach((hash, index) => {
                callback(this._processingMap.get(group).storageMap[hash].map, hash, index);
            });
    }

    forEachUsedGroups(group, callback) {
        this._processingMap.get(group).usedGroups
            .forEach((name, index) => {
                callback(name, index);
            });
    }

    destroyStorageMap(group, hash) {
        delete this._processingMap.get(group).storageMap[hash];
    }

    getMap(group, hash) {
        return this._processingMap.get(group).storageMap[hash]
            && this._processingMap.get(group).storageMap[hash].map;
    }

    checkStorageMap(group, hash) {
        if (!this._processingMap.get(group).storageMap[hash]) {
            this._processingMap.get(group).storageMap[hash] = this.makeStorageMap();
        }
    }

    async getStorageMap(group, hash, session, stage, key) {
        const storageMap = this._processingMap.get(group).storageMap[hash];

        if (!storageMap.map) {
            const mapper = this.getMapper(stage);
            storageMap.map = await this.makeMap(group, session, key, mapper);
        }

        return storageMap;
    }

    makeProcessingMap() {
        return {
            processes: 0,
            storageMap: {},
            usedGroups: [],
        };
    }

    checkProcessingMap(group) {
        if (!this._processingMap.get(group)) {
            this._processingMap.set(group, this.makeProcessingMap());
        }
    }

    // TODO: Catch errors
    async onData(_socket, { session, group, stage, key, data }, _getHash = getHash, _getId = getId) {
        const hash = _getHash(session, stage, key || _getId());
        this.checkProcessingMap(group);
        this._processingMap.get(group).processes++;
        this.checkStorageMap(group, hash);
        const storageMap = await this.getStorageMap(group, hash, session, stage, key);
        await storageMap.map.onData({ stage, key, data, eof: !data });
        if (!key) {
            this.destroyStorageMap(group, hash);
        }
        this._processingMap.get(group).processes--;
    }
}
