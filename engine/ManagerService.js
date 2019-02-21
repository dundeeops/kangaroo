const { pipeline, Transform } = require("stream");
const es = require("event-stream");
const {
    serializeData,
    getId,
    getHash,
} = require("./SerializationUtil.js");
const EventStreamTransformStream = require("./EventStreamTransformStream.js");
const SendWritableStream = require("./SendWritableStream.js");
const OrchestratorServicePrototype = require("./OrchestratorServicePrototype.js");

module.exports = class ManagerService extends OrchestratorServicePrototype {

    constructor(options) {
        super(options);
    }

    runStream(stage, stream, key) {
        const session = getId();

        return pipeline(
            stream,
            this.getLinesStream(),
            this.getSerializationStream(session, stage),
            this.getOutcomeStream(session, stage, key),
            this.errorProcessing,
        );
    }

    getLinesStream() {
        return new EventStreamTransformStream();
    }

    getSerializationStream(session, stage, key) {
        return new Transform({
            transform(chunk, encoding, callback) {
                const data = chunk.toString();
                this.push(serializeData({ session, stage, key, data }));
                callback();
            },
            final(callback) {
                this.push(serializeData({ session, stage, key, data: null }));
                callback();
            }
        });
    }

    // TODO: error processing
    errorProcessing(err) {
        if (err) {
            console.error("Pipeline failed.", err);
        }
    }

    getOutcomeStream(session, stage, key) {
        return new SendWritableStream({
            send: this.send.bind(this),
            group: getHash(session, stage),
            session, stage, key
        });
    }
}
