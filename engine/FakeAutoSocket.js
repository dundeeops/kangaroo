const EventEmitter = require("events");
const {
    serializeData,
} = require("./SerializationUtil.js");
const Dict = require("./AskDict.js");

const sendSocketInfo = (socket, mappers = []) => socket.emit("data", serializeData({
    type: "info", mappers,
}) + Dict.ENDING);

module.exports = class FakeAutoSocket extends EventEmitter {
    constructor() {
        super();
    }

    connect() {
        this.emit("connect", true);
        sendSocketInfo(this);
    }

    write(data) {
        this.emit("write", data);
        return true;
    }

    destroy() {
        this.emit("destroy", true);
    }
};
