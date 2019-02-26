const EventEmitter = require("events");
const {Readable} = require("stream");

module.exports = class FakeSocket extends Readable {
    constructor() {
        super();
    }

    connect() {
        this.emit("connect", true);
    }

    write(data) {
        this.emit("write", data);
        return true;
    }

    _read() {}

    destroy() {
        this.emit("destroy", true);
    }
};
