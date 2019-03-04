const EventEmitter = require("events");
const {
    serializeData,
} = require("./SerializationUtil.js");
const BaseDict = require("./BaseDict.js");

const sendSocketInfo = (socket, mappers = ["testStage"]) => socket.emit(
    "data",
    serializeData({
        type: "info", mappers,
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

    destroy() {
        this.emit("destroy", true);
    }
};
