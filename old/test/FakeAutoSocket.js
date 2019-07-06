const EventEmitter = require("events");
const {
    serializeData,
} = require("../engine/SerializationUtil.js");
const AskDict = require("../engine/AskDict.js");
const BaseDict = require("../engine/BaseDict.js");

const sendSocketInfo = (socket, mappers = ["testStage"]) => socket.emit(
    "data",
    serializeData({
        type: AskDict.INFO,
        mappers,
    }) + BaseDict.ENDING,
);

module.exports = class FakeAutoSocket extends EventEmitter {
    constructor() {
        super();
        this._fakeQueue = [];
    }

    connect() {
        this.emit("connect", true);
        sendSocketInfo(this);
    }

    write(data) {
        this.emit("write", data);
        this._fakeQueue.push(data);
        return true;
    }

    close() {
        this.emit("close", true);
    }
};
