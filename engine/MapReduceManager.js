const { pipeline, Transform } = require("stream");
const {
    serializeData,
} = require("./Serialization.js");
const MapReduceOrchestrator = require("./MapReduceOrchestrator.js");

module.exports = class MapReduceManager extends MapReduceOrchestrator {

    constructor(options) {
        super(options);
    }

    runStream(stage, stream) {
        const session = this.getId();

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
