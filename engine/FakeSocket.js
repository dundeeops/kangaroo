const EventEmitter = require("events");

module.exports = class FakeSocket extends EventEmitter {
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

    destroy() {
        this.emit("destroy", true);
    }
};
