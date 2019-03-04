const path = require("path");
const tar = require("tar");
const child_process = require("child_process");
const {
    makeMessage,
    getId,
    getHash,
} = require("./SerializationUtil.js");
const {
    deleteFolderRecursive,
    makeDirIfNotExist,
} = require("./FsUtil");
const {
    getPromise,
    repeatIfTrueTimeout,
} = require("./PromiseUtil");
const OrchestratorServicePrototype = require("./OrchestratorServicePrototype.js");
const WorkerServer = require("./WorkerServer.js");
const AskDict = require("./AskDict.js");

const defaultOptions = {
    hostname: "localhost",
    port: 2325,
    modulesPath: path.resolve("./upload"),
    inject: {
        _repeatIfTrueTimeout: repeatIfTrueTimeout,
        _WorkerServer: WorkerServer,
    },
};

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
        this.initServer(options);
        this.initOnAskMap();
    }

    initInjections(options) {
        this._repeatIfTrueTimeout = options.inject._repeatIfTrueTimeout;
        this._WorkerServer = options.inject._WorkerServer;
    }

    initWorker() {
        this._mappers = new Map();
        this._processingMap = new Map();
        this._staticMappersMap = new Map();
    }

    initServer(options) {
        this._server = new this._WorkerServer({
            hostname: options.hostname,
            port: options.port,
            onAsk: this.onAsk.bind(this),
            onData: this.onData.bind(this),
        });
    }

    initOnAskMap() {
        this.onAskMap = {
            [AskDict.GET_SESSION_STAGE_KEY_SERVER]: this.onAskGetSessionStageKeyServer.bind(this),
            [AskDict.CAN_GET_STAGE]: this.onAskCanGetStage.bind(this),
            [AskDict.IS_PROCESSING]: this.onAskIsProcessing.bind(this),
            [AskDict.NULL_ACHIEVED]: this.onAskNullAchieved.bind(this),
            [AskDict.UPLOAD]: this.onUpload.bind(this),
            [AskDict.STATIC_MODULES_STATUS]: this.onAskStaticModulesStatus.bind(this),
        };
    }
    
    onAskGetSessionStageKeyServer(socket, id, type, data, _makeMessage = makeMessage) {
        const serverName = this.getSessionStageKeyServer(data.session, data.stage, data.key);
        socket.write(
            _makeMessage({
                id,
                type,
                data: serverName,
            }),
        );
    }
    
    onAskIsProcessing(socket, id, type, data, _makeMessage = makeMessage) {
        socket.write(
            _makeMessage({
                id,
                type,
                data: this._processingMap.get(data.group)
                    && this._processingMap.get(data.group).processes
                        ? true
                        : null,
            }),
        );
    }

    onAskCanGetStage(socket, id, type, {stage}, _makeMessage = makeMessage) {
        const mappers = this.getMappers();
        const isContains = mappers.includes(stage);
        socket.write(
            _makeMessage({
                id,
                type,
                data: isContains
                    ? true
                    : null,
            }),
        );
    }

    async onAskStaticModulesStatus(socket, id, type, data, _makeMessage = makeMessage) {
        let staticMapper;
        this._staticMappersMap.forEach((mapper) => {
            if (mapper.info.id === data.id) {
                staticMapper = mapper;
            }
        });

        if (staticMapper) {
            socket.write(_makeMessage({
                id,
                type,
                data: {
                    isLoading: staticMapper.isLoading,
                    id: staticMapper.info.id,
                    mappers: staticMapper.info.mappers,
                },
            }));
        }
    }

    // TODO: Split onUpload
    async onUpload(socket, _id, type, data, _deleteFolderRecursive = deleteFolderRecursive, _makeDirIfNotExist = makeDirIfNotExist) {
        const {id, info, bytes} = data;
        let obj = this._staticMappersMap.get(id);

        if (!obj) {
            const [promise, resolve] = getPromise();
            this._staticMappersMap.set(id, {
                count: 0, promise, resolve,
            });
            obj = this._staticMappersMap.get(id);
            obj.isLoading = true;
        }

        obj.count++;

        if (info) {
            const dir = path.resolve(this._modulesPath, id);
            await _deleteFolderRecursive(dir);
            await _makeDirIfNotExist(dir);

            const stream = tar.x({
                C: dir,
                sync: true,
            });

            obj.info = info;
            obj.stream = stream;
            obj.dir = dir;
        }
        
        if (!bytes) {
            await obj.promise;
            obj.stream.end();
            delete obj.stream;
            // TODO: Store info
            // fs.writeFileSync(infoPath, JSON.stringify(obj.info));
            obj.npmInstall = child_process.exec(
                obj.info.init || "npm install --production",
                    {
                    cwd: obj.dir,
                }, (error, stdout, stderr) => {
                    // TODO: Emit logs
                },
            );
            obj.npmInstall.stderr.pipe(process.stderr);
            obj.npmInstall.on("end", () => {
                delete obj.npmInstall;
                obj.isLoading = false;
            });
        } else {
            obj.stream.write(Buffer.from(bytes));
            obj.count--;
            if (obj.count === 0) {
                obj.resolve();
            }
        }
    }

    async onAskNullAchieved(socket, id, type, data) {
        await this._repeatIfTrueTimeout(async () => {
            if (this._processingMap.get(data.group)) {
                const isReady = !this._processingMap.get(data.group).processes
                    && !await this._connectionService.ask(AskDict.IS_PROCESSING, { group: data.group });

                if (isReady) {
                    this.forEachStorageMaps(data.group, (map) => {
                        map.onFinish();
                    });

                    this.forEachUsedGroups(data.group, (nextGroup) => {
                        this._connectionService.notify(AskDict.NULL_ACHIEVED, {
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
        await this.checkStaticPathExist();
        await this._server.start();
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

    getStaticMapper(stage) {
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

        return require(path.resolve(resultStaticMap.dir, resultEntry))[stage];
    }

    getMapper(stage) {
        let mapper = this._mappers.get(stage);
        if (!mapper) {
            mapper = this.getStaticMapper(stage);
        }
        return mapper;
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
