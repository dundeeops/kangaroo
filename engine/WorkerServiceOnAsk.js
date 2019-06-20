const path = require("path");
const tar = require("tar");
const child_process = require("child_process");
const {
    makeMessage,
} = require("./SerializationUtil.js");
const {
    deleteFolderRecursive,
    makeDirIfNotExist,
} = require("./FsUtil");
const {
    getPromise,
} = require("./PromiseUtil");
const AskDict = require("./AskDict.js");
const TimeoutErrorTimer = require("./TimeoutErrorTimer.js");

const defaultOptions = {
    workerService: null,
    inject: {
        _TimeoutErrorTimer: TimeoutErrorTimer,
    },
};

// TODO: Make delete module
// TODO: Make delete all modules
// TODO: Make stop/start processes
// TODO: Make processing statuses
// TODO: Remove private linking
module.exports = class WorkerServiceOnAsk {
    constructor(_options = {}) {
        const options = {
            ...defaultOptions,
            ..._options,
        };
        this._workerService = options.workerService;

        this.initInjections(options);
        this.initOnAskMap();
    }

    initInjections(options) {
        this._TimeoutErrorTimer = options.inject._TimeoutErrorTimer;
    }

    initOnAskMap() {
        this._onAskMap = {
            [AskDict.GET_SESSION_STAGE_KEY_SERVER]: this.onAskGetSessionStageKeyServer.bind(this),
            [AskDict.CAN_GET_STAGE]: this.onAskCanGetStage.bind(this),
            [AskDict.IS_PROCESSING]: this.onAskIsProcessing.bind(this),
            [AskDict.NULL_ACHIEVED]: this.onAskNullAchieved.bind(this),
            [AskDict.END_PROCESSING]: this.onAskEndProcessing.bind(this),
            [AskDict.COUNT_PROCESSED]: this.onAskCountProcessed.bind(this),
            [AskDict.UPLOAD]: this.onUpload.bind(this),
            [AskDict.STATIC_MODULES_STATUS]: this.onAskStaticModulesStatus.bind(this),
        };
    }

    onAskGetSessionStageKeyServer(data, _makeMessage = makeMessage) {
        return this._workerService.getSessionStageKeyServer(data.session, data.stage, data.key);
    }
    
    onAskIsProcessing(data, _makeMessage = makeMessage) {
        return this._workerService._processingMap.get(data.group)
            && this._workerService._processingMap.get(data.group).processes
                ? true
                : null
    }

    onAskCanGetStage({ stage }) {
        const mappers = this._workerService.getMappers();
        const isContains = mappers.includes(stage);
        return isContains
            ? true
            : null;
    }

    async onAskStaticModulesStatus(data) {
        let staticMapper;
        this._workerService._staticMappersMap.forEach((mapper) => {
            if (mapper.info.id === data.id) {
                staticMapper = mapper;
            }
        });

        if (staticMapper) {
            return {
                isLoading: staticMapper.isLoading,
                id: staticMapper.info.id,
                mappers: staticMapper.info.mappers,
            };
        }
    }

    // TODO: Split onUpload
    async onUpload(data, _deleteFolderRecursive = deleteFolderRecursive, _makeDirIfNotExist = makeDirIfNotExist) {
        const {id, info, bytes} = data;
        let obj = this._workerService._staticMappersMap.get(id);

        if (!obj) {
            const [promise, resolve] = getPromise();
            this._workerService._staticMappersMap.set(id, {
                count: 0, promise, resolve,
            });
            obj = this._workerService._staticMappersMap.get(id);
            obj.isLoading = true;
        }

        obj.count++;

        if (info) {
            const dir = path.resolve(this._workerService._modulesPath, id);
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

    async onAskCountProcessed({ group }) {
        const map = this._workerService._processingMap.get(group);
        if (map) {
            return map.processed;
        }
    }

    async onAskEndProcessing({ group }) {
        const map = this._workerService._processingMap.get(group);
        if (map) {
            this._workerService.forEachStorageMaps(group, (map) => {
                map.onFinish();
            });

            this._workerService.forEachUsedGroups(group, (nextGroup, totalSum) => {
                this._workerService._connectionService.notify(AskDict.NULL_ACHIEVED, {
                    group: nextGroup,
                    totalSum,
                });
            });

            this._workerService._processingMap.delete(group);
        }
    }

    async onAskNullAchieved(data) {
        if (this._workerService._processingMap.get(data.group)) {
            this._workerService._processingMap.get(data.group).totalSum = data.totalSum;

            // let timeout;
            // const onEndProcessing = async (group) => {
            //     console.log(this._workerService._processingMap.get(data.group).processes);
                
            //     const isReady = group === data.group && !this._workerService._processingMap.get(data.group).processes
            //         && !await this._workerService._connectionService.ask(AskDict.IS_PROCESSING, { group: data.group })
            //         && !!this._workerService._processingMap.get(group);

            //     if (isReady) {
            //         this._workerService.forEachStorageMaps(data.group, (map) => {
            //             map.onFinish();
            //         });

            //         this._workerService.forEachUsedGroups(data.group, (nextGroup) => {
            //             this._workerService._connectionService.notify(AskDict.NULL_ACHIEVED, {
            //                 group: nextGroup,
            //             });
            //         });

            //         console.log('ends', isReady, data.group, this._workerService._processingMap.get(data.group).processes);
            //         this._workerService._processingMap.delete(data.group);
            //         this._workerService.off(AskDict.END_PROCESSING, onEndProcessing);
            //         timeout.stop();
            //     }
            // };
            // timeout = new this._TimeoutErrorTimer({
            //     onError: () => {
            //         this._workerService.off(AskDict.END_PROCESSING, onEndProcessing);
            //     },
            // });
            // this._workerService.on(AskDict.END_PROCESSING, onEndProcessing);
            // timeout.start();
            // await onEndProcessing(data.group);
        }
    }

    async onAsk(socket, { id, type, data, isAsk }, _makeMessage = makeMessage) {
        const answer = await this._onAskMap[type](data);
        if (isAsk) {
            socket.write(
                _makeMessage({
                    id,
                    type,
                    data: answer,
                }),
            );
        }
    }
}
