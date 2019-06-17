const path = require("path");
const {
    getId,
    getHash,
} = require("./SerializationUtil.js");
const {
    makeDirIfNotExist,
} = require("./FsUtil");
const OrchestratorServicePrototype = require("./OrchestratorServicePrototype.js");
const WorkerServer = require("./WorkerServer.js");
const WorkerServiceOnAsk = require("./WorkerServiceOnAsk.js");
const AskDict = require("./AskDict.js");

const NO_MODULE_FOUND_ERROR = "Can't find a module in local storage at $0 with id $1 for stage $2";

const defaultOptions = {
    managerServer: {
        hostname: "localhost",
        port: 2325,
    },
    dataServer: {
        hostname: "localhost",
        port: 2325,
    },
    modulesPath: path.resolve("./upload"),
    onError: () => {},
    inject: {
        _WorkerServer: WorkerServer,
        _WorkerServiceOnAsk: WorkerServiceOnAsk,
    },
};

// TODO: Make stop/start processing
// TODO: Make wait all unfinished processes
// TODO: Make error processing
// TODO: Make accumulator
module.exports = class WorkerService extends OrchestratorServicePrototype {
    constructor(_options = {}) {
        const options = {
            ...defaultOptions,
            ..._options,
            inject: {
                ...defaultOptions.inject,
                ..._options.inject,
            },
        };
        super(options);
        this._modulesPath = options.modulesPath;

        this.initInjections(options);
        this.initWorker(options);
        this.initOnAskMap();
        this.initServer(options);
    }

    initInjections(options) {
        this._WorkerServer = options.inject._WorkerServer;
        this._WorkerServiceOnAsk = options.inject._WorkerServiceOnAsk;
    }

    initWorker() {
        this._mappers = new Map();
        this._processingMap = new Map();
        this._staticMappersMap = new Map();
    }

    initOnAskMap() {
        this._workerServiceOnAsk = new this._WorkerServiceOnAsk({
            workerService: this,
        });
    }

    initServer(options) {
        this._managerServer = new this._WorkerServer({
            hostname: options.managerServer.hostname,
            port: options.managerServer.port,
            onData: this._workerServiceOnAsk.onAsk.bind(this._workerServiceOnAsk),
            onError: options.onError,
        });
        this._dataServer = new this._WorkerServer({
            hostname: options.dataServer.hostname,
            port: options.dataServer.port,
            onData: this.onData.bind(this),
            onError: options.onError,
        });
    }

    async start() {
        await this.checkStaticPathExist();
        await this._managerServer.start();
        await this._dataServer.start();
    }

    async checkStaticPathExist(_makeDirIfNotExist = makeDirIfNotExist) {
        await _makeDirIfNotExist(this._modulesPath);
    }

    getMappers() {
        let mappers = Array.from(this._mappers.keys());

        this._staticMappersMap
            .forEach((staticMapper) => {
                Object
                    .keys(staticMapper.info.mappers)
                    .forEach((entry) => {
                        mappers = mappers.concat(staticMapper.info.mappers[entry]);
                    })
            });

        return mappers;
    }

    setMapper(stage, map) {
        this._mappers.set(stage, map);
    }

    getStaticMapperScript(stage) {
        let resultStaticMap = null;
        let resultEntry = null;

        Array.from(this._staticMappersMap.values())
            .find((staticMap) => {
                let entry = Object
                    .keys(staticMap.info.mappers)
                    .find((entry) => staticMap.info.mappers[entry].includes(stage));
                if (entry) {
                    resultStaticMap = staticMap;
                    resultEntry = entry;
                }
                return !!entry;
            });

        if (resultStaticMap && resultStaticMap.dir && resultEntry) {
            const staticModule = require(
                path.resolve(resultStaticMap.dir, resultEntry),
            );

            return staticModule[stage];
        }

        throw Error(
            NO_MODULE_FOUND_ERROR
                .replace("$0", resultStaticMap
                    && path.resolve(resultStaticMap.dir, resultEntry))
                .replace("$1", resultStaticMap
                    && resultStaticMap.info
                    && resultStaticMap.info.id)
                .replace("$2", stage),
        );
    }

    getMapperScript(stage) {
        let mapper = this._mappers.get(stage);
        if (!mapper) {
            mapper = this.getStaticMapperScript(stage);
        }
        return mapper;
    }

    getSendCatchUsedGroupWrap(group, session, _getHash = getHash) {
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
        const sendWrap = this.getSendCatchUsedGroupWrap(group, session);
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
        this._processingMap.get(group).storageMap
            .forEach((storageMap, hash) => {
                callback(storageMap.map, hash);
            });
    }

    forEachUsedGroups(group, callback) {
        this._processingMap.get(group).usedGroups
            .forEach((name, index) => {
                callback(name, index);
            });
    }

    destroyStorageMap(group, hash) {
        delete this._processingMap.get(group).storageMap.get(hash);
    }

    getMap(group, hash) {
        const storageMap = this._processingMap.get(group).storageMap.get(hash);
        return storageMap && storageMap.map;
    }

    checkStorageMap(group, hash) {
        const processingMap = this._processingMap.get(group);
        if (!processingMap.storageMap.get(hash)) {
            processingMap.storageMap.set(hash, this.makeStorageMap());
        }
    }

    async getStorageMap(group, hash, session, stage, key) {
        const storageMap = this._processingMap.get(group).storageMap.get(hash);

        if (!storageMap.map) {
            const mapper = this.getMapperScript(stage);
            storageMap.map = await this.makeMap(group, session, key, mapper);
        }

        return storageMap;
    }

    makeProcessingMap() {
        return {
            processes: 0,
            storageMap: new Map(),
            usedGroups: [],
        };
    }

    checkProcessingMap(group) {
        if (!this._processingMap.get(group)) {
            this._processingMap.set(group, this.makeProcessingMap());
        }
    }

    // TODO: Catch errors & restore accumulators
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
        if (this._processingMap.get(group).processes === 0) {
            this._connectionService.notify(AskDict.END_PROCESSING, {
                group,
            });
        }
    }
}
