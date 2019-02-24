const es = require("event-stream");
const {
    getId,
    getHash,
} = require("./SerializationUtil.js");
const OrchestratorServicePrototype = require("./OrchestratorServicePrototype.js");
const Dict = require("./AskDict.js");

const defaultOptions = {};

module.exports = class ManagerService extends OrchestratorServicePrototype {

    constructor(_options) {
        const options = {
            ...defaultOptions,
            ..._options,
        };
        super(options);
    }

    runStream(stage, stream, key, _getId = getId, _getHash = getHash, _es = es) {
        const session = _getId();
        const group = _getHash(session, stage);

        return stream
            .pipe(_es.split())
            .pipe(_es.map((data, callback) => {
                this.send(session, group, stage, key, data);
                callback(null, null);
            }))
            .on("end", () => {
                this.notify(Dict.NULL_ACHIVED, { group });
            });
    }
}
