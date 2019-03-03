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

    uploadModuleStream(readStream, info) {
        this._connectionService
            .getConnections()
            .forEach((connection) => {
                readStream.pipe(connection.getUploadFileStream(info));
            });
        return readStream;
    }

    async getStaticModulesStatus(id) {
        return await this.askAll(Dict.STATIC_MODULES_STATUS, { id });
    }

    runStream(stage, stream, key, _getId = getId, _getHash = getHash, _es = es) {
        const session = _getId();
        const group = _getHash(session, stage);

        return stream
            .pipe(_es.split())
            .pipe(_es.map(async (data, callback) => {
                await this.send(session, group, stage, key, data);
                callback(null, null);
            }))
            .on("end", () => {
                this.notify(Dict.NULL_ACHIVED, { group });
            });
    }
}
