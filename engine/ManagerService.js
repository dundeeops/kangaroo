const es = require("event-stream");
const {
    getId,
    getHash,
} = require("./SerializationUtil.js");
const OrchestratorServicePrototype = require("./OrchestratorServicePrototype.js");
const Dict = require("./AskDict.js");

const defaultOptions = {};

module.exports = class ManagerService extends OrchestratorServicePrototype {

    constructor(_options = {}) {
        const options = {
            ...defaultOptions,
            ..._options,
        };
        super(options);
    }

    uploadModuleStream(readStream, data) {
        return this._connectionService.uploadModuleStream(readStream, data);
    }

    async getStaticModulesStatus(id) {
        return await this._connectionService.askAll(Dict.STATIC_MODULES_STATUS, { id });
    }

    runStream(stage, key, stream, _getId = getId, _getHash = getHash, _es = es) {
        const session = _getId();
        const group = _getHash(session, stage);
        let totalSum = 0;
        return stream
            .pipe(_es.split())
            .pipe(_es.map(async (data, callback) => {
                totalSum++;
                await this.send(
                    session,
                    group,
                    stage,
                    key,
                    data,
                );
                callback(null, data);
            }))
            .on("end", () => {
                this._connectionService.notify(
                    Dict.NULL_ACHIEVED,
                    {
                        group,
                        totalSum,
                    },
                );
            });
    }
}
