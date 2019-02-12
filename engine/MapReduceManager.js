const { pipeline, Transform } = require("stream");
const {
    serializeData,
} = require("./Serialization.js");
const MapReduceBase = require("./MapReduceOrchestrator.js");

module.exports = class MapReduceManager extends MapReduceBase {

    constructor(options) {
        super(options);
    }

    runManagerStream(stage, stream) {
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
                this.push(serializeData(session, stage, key, data));
                callback();
            },
        });
    }
}
