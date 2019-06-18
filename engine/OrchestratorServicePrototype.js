const EventEmitter = require("events");
const {
    makeMessage,
    getHash,
} = require("./SerializationUtil.js");
const AskDict = require("./AskDict.js");
const {
    getPromise,
} = require("./PromiseUtil.js");

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
        this._connectionCacheMap = new Map();
    }

    async send(session, group, stage, key, data) {
        const connectionKey = await this.getSessionStageKeyConnectionScript(
            session,
            stage,
            key,
        );
        await this.sendToServer(
            connectionKey,
            session,
            group,
            stage,
            key,
            data,
        );
        return connectionKey;
    }

    async sendToServer(connectionKey, session, group, stage, key, data, _makeMessage = makeMessage) {
        const connection = this._connectionService.getConnection(connectionKey);
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

    setSessionStageKeyServer(session, stage, key, data, _getHash = getHash) {
        const hash = _getHash(session, stage, key);
        this._sessionStageKeyMap.set(hash, data);
    }

    async getSessionStageKeyConnectionScript(session, stage, key) {
        let connectionKey;

        if (key) {
            connectionKey = await this.askSessionStageKeyServer(session, stage, key);
        }

        if (!connectionKey) {
            const connection = await this.findStageConnection(stage);
            connectionKey = connection && connection.key;
        }

        if (!connectionKey) {
            throw Error(NO_CONNECTIONS_ERROR);
        }

        if (key) {
            this.setSessionStageKeyServer(session, stage, key, { connectionKey });
        }

        return connectionKey;
    }

    async askSessionStageKeyServer(session, stage, key, _getPromise = getPromise) {
        let sessionKeyCache = this.getSessionStageKeyServer(session, stage, key);
        if (sessionKeyCache && sessionKeyCache.promise) {
            await sessionKeyCache.promise;
            sessionKeyCache = this.getSessionStageKeyServer(session, stage, key);
        }
        if (!sessionKeyCache) {
            const [promise, resolve] = _getPromise();
            this.setSessionStageKeyServer(session, stage, key, { promise, resolve });
            const answer = await this._connectionService
                .ask(
                    AskDict.GET_SESSION_STAGE_KEY_SERVER,
                    {
                        session,
                        stage,
                        key,
                    },
                );
            const connection = answer && answer.connection;
            sessionKeyCache = {
                connectionKey: connection && connection.key,
            };
            this.setSessionStageKeyServer(session, stage, key, sessionKeyCache);
            resolve();
        }
        return sessionKeyCache.connectionKey;
    }

    async findStageConnection(stage, _getPromise = getPromise) {
        let connectionCache = this._connectionCacheMap.get(stage);
        if (connectionCache && connectionCache.promise) {
            await connectionCache.promise;
            connectionCache = this._connectionCacheMap.get(stage);
        }
        if (!connectionCache || (new Date() - connectionCache.date) > 10 * 1000) {
            const [promise, resolve] = _getPromise();
            connectionCache = {
                promise,
                resolve,
            };
            this._connectionCacheMap.set(stage, connectionCache);
            const answers = await this._connectionService.askLimit(AskDict.CAN_GET_STAGE, {
                stage,
            });
            const connections = answers.map(c => c.connection);
            connectionCache = {
                date: +new Date(),
                connections,
            };
            this._connectionCacheMap.set(stage, connectionCache);
            resolve(answers);
        }
        
        return connectionCache.connections[Math.floor(Math.random() * connectionCache.connections.length)];
    }
}
