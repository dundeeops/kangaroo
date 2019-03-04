const FakeTimeoutErrorTimer = require("./FakeTimeoutErrorTimer.js");
const FakeRestartService = require("./FakeRestartService.js");
const BaseDict = require("./BaseDict.js");
const ConnectionSocket = require("./ConnectionSocket.js");
const FakeSocket = require("./FakeSocket.js");
const {
    serializeData,
} = require("./SerializationUtil.js");
const {
    startUnlessTimeout,
} = require("./PromiseUtil.js");

const sendSocketInfo = (socket, mappers = []) => socket.emit("data", serializeData({
    type: "info", mappers,
}) + BaseDict.ENDING);

class FakeConnectionSocket extends ConnectionSocket {
    constructor(options) {
        super({
            hostname: "test",
            port: 3333,
            inject: {
                _net: {
                    Socket: FakeSocket,
                },
                _TimeoutErrorTimer: FakeTimeoutErrorTimer,
                _RestartService: FakeRestartService,
            },
            ...options,
        });
    }
}

const connectionSocketFactory = (onConnect, autoFakeConnect) => {
    const connectionSocket = new FakeConnectionSocket({
        onConnect,
        autoFakeConnect,
    });
    return connectionSocket;
}

const waitSocket = (connectionSocket, getSocket, callback, mappers = []) => startUnlessTimeout(() => {
    const socket = getSocket();
    if (!socket) {
        return true;
    } else {
        sendSocketInfo(socket, mappers);
    }

    if (connectionSocket.isAlive()) {
        callback();
        return false;
    } else {
        return true;
    }
}, 100);

module.exports = {
    FakeConnectionSocket,
    connectionSocketFactory,
    sendSocketInfo,
    waitSocket,
};