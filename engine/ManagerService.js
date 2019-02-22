const es = require("event-stream");
const {
    getId,
    getHash,
} = require("./SerializationUtil.js");
const OrchestratorServicePrototype = require("./OrchestratorServicePrototype.js");

module.exports = class ManagerService extends OrchestratorServicePrototype {

    constructor(options) {
        super(options);
    }

    runStream(stage, stream, key) {
        const session = getId();
        const group = getHash(session, stage);

        return stream
            .pipe(es.split())
            .pipe(es.map((data, cb) => {
                this.send(session, group, stage, key, data);
                cb(null, null);
            }))
            .on("end", () => {
                this.notify("nullAchived", { group });
            });
    }
}
