const { pipeline, Transform } = require("stream");
const {
    serializeData,
    getId,
} = require("./SerializationUtil.js");
const OrchestratorServicePrototype = require("./OrchestratorServicePrototype.js");

module.exports = class ManagerService extends OrchestratorServicePrototype {

    constructor(options) {
        super(options);
    }

    runStream(stage, stream) {
        const session = getId();

        // TODO: error processing
        return pipeline(
            stream,
            this.getLinesStream(),
            this.getSerializationStream(session, stage),
            this.getOutcomeStream(session, stage),
            this.errorProcessing,
        );
    }

    getSerializationStream(session, stage, key) {
        return new Transform({
            transform(chunk, encoding, callback) {
                const data = chunk.toString();
                this.push(serializeData({ session, stage, key, data }));
                callback();
            },
        });
    }
}
