const BaseDict = require("../engine/BaseDict.js");
const ConnectionSocket = require("../engine/ConnectionSocket.js");
const {
    serializeData,
} = require("../engine/SerializationUtil.js");
const {
    repeatIfTrueTimeout,
} = require("../engine/PromiseUtil.js");
const AskDict = require("../engine/AskDict.js");
const FakeTimeoutErrorTimer = require("./FakeTimeoutErrorTimer.js");
const FakeRestartService = require("./FakeRestartService.js");
const FakeSocket = require("./FakeSocket.js");

const sendSocketInfo = (
    socket,
    mappers = [],
) => socket.emit("data", serializeData({
    type: AskDict.INFO,
    mappers,
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

const waitSocket = (connectionSocket, getSocket, callback, mappers = []) => repeatIfTrueTimeout(() => {
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