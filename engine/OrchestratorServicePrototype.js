const EventEmitter = require("events");
const {
    makeMessage,
    getHash,
} = require("./SerializationUtil.js");
const AskDict = require("./AskDict.js");

const NO_CONNECTIONS_ERROR = "There are no alive servers";

const defaultOptions = {};

module.exports = class OrchestratorServicePrototype extends EventEmitter {
    constructor(_options = {}) {
        super();
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
        const serverName = await this.getSessionStageKeyConnectionScript(
            session,
            stage,
            key,
        );
        await this.sendToServer(
            serverName,
            session,
            group,
            stage,
            key,
            data,
        );
        return serverName;
    }

    async sendToServer(serverName, session, group, stage, key, data, _makeMessage = makeMessage) {
        const connection = this._connectionService.getConnection(serverName);
        const message = _makeMessage({
            session,
            group,
            stage,
            key,
            data,
        });
        return connection.push(message);
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
        return await this._connectionService
            .ask(
                AskDict.GET_SESSION_STAGE_KEY_SERVER,
                {
                    session,
                    stage,
                    key,
                },
            );
    }

    async getSessionStageKeyConnectionScript(session, stage, key) {
        let connectionKey;

        if (key) {
            connectionKey = this.getSessionStageKeyServer(session, stage, key);
            if (!connectionKey) {
                connectionKey = await this.askSessionStageKeyServer(session, stage, key);
            }
        }

        if (!connectionKey) {
            // TODO: Cache responses for a short of time
            const connection = await this.findStageConnection(stage);
            if (connection) {
                connectionKey = connection.key;
            }
        }

        if (!connectionKey) {
            throw Error(NO_CONNECTIONS_ERROR);
        }

        if (key) {
            this.setSessionStageKeyServer(session, stage, key, connectionKey);
        }

        return connectionKey;
    }

    async findStageConnection(stage) {
        const connection = await this._connectionService.findConnection(AskDict.CAN_GET_STAGE, {
            stage,
        });

        return connection;
    }
}
