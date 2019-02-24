const {
    serializeData,
    getHash,
} = require("./SerializationUtil.js");
const {
    raceData,
} = require("./PromisifyUtil");
const Dict = require("./AskDict.js");

const NO_CONNECTIONS_ERROR = "There are no alive servers";

const defaultOptions = {};

// TODO: Make connection chooser (prefer by a ping and with minimum CPU loadout) & Add a custom picking function
module.exports = class OrchestratorServicePrototype {
    constructor(_options) {
        const options = {
            ...defaultOptions,
            ..._options,
        };
        this._connectionService = options.connectionService;

        this.init(options);
    }

    init() {
        this._sessionStageKeyMap = new Map();
    }

    async send(session, group, stage, key, data) {
        const serverName = await this.getSessionStageKeyConnection(session, stage, key);
        await this.sendToServer(serverName, session, group, stage, key, data);
        return serverName;
    }

    async sendToServer(serverName, session, group, stage, key, data, _serializeData = serializeData) {
        const connection = this._connectionService.getConnection(serverName);
        const raw = _serializeData({ session, group, stage, key, data });
        return connection.sendData(raw + Dict.ENDING);
    }

    getSessionStageKeyServer(session, stage, key, _getHash = getHash) {
        const hash = getHash(session, stage, key);
        return this._sessionStageKeyMap.get(hash);
    }

    setSessionStageKeyServer(session, stage, key, server, _getHash = getHash) {
        const hash = _getHash(session, stage, key);
        this._sessionStageKeyMap.set(hash, server);
    }

    async askSessionStageKeyServer(session, stage, key) {
        return await this.ask(Dict.GET_SESSION_STAGE_KEY_SERVER, {
            session, stage, key,
        });
    }

    async getSessionStageKeyConnection(session, stage, key) {
        let serverName;

        if (key) {
            serverName = this.getSessionStageKeyServer(session, stage, key);
            if (!serverName) {
                serverName = await this.askSessionStageKeyServer(session, stage, key);
            }
        }

        if (!serverName) {
            serverName = await this.getRandomAliveConnection(stage);
        }

        if (key) {
            this.setSessionStageKeyServer(session, stage, key, serverName);
        }

        return serverName;
    }

    async notify(type, data) {
        const promises = [];
        this._connectionService
            .getConnections()
            .forEach((connection) => {
                promises.push(connection.notify(type, data));
            });
        await Promise.all(promises);
    }

    async ask(type, data, _raceData = raceData) {
        const promises = [];
        this._connectionService
            .getConnections()
            .forEach((connection) => {
                promises.push(connection.ask(type, data));
            });
        return await _raceData(promises);
    }

    shuffle(a) {
        for (let i = a.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [a[i], a[j]] = [a[j], a[i]];
        }
        return a;
    }

    // TODO: Make connection orchestration
    getRandomSortedAliveConnections(stage) {
        return this.shuffle(
            Array.from(
                this._connectionService
                    .getConnections()
                    .values()
            )
            .filter((connection) => {
                return connection.isContainsStage(stage) && connection.isAlive();
            })
            .map((connection) => connection.getName())
        );
    }

    async getRandomAliveConnection(stage) {
        const sorted = this.getRandomSortedAliveConnections(stage);

        if (sorted.length === 0) {
            throw Error(NO_CONNECTIONS_ERROR);
        }

        return sorted[0];
    }
}
