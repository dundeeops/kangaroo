const EventEmitter = require("./EventEmitter.js");
const {
    serializeData,
    getHash,
} = require("./SerializationUtil.js");

module.exports = class OrchestratorServicePrototype extends EventEmitter {

    constructor(options) {
        super();
        this._connectionService = options.connectionService;
        // this._preferableServerName = options.preferableServerName;

        this._sessionStageKeyMap = {};
    }

    async send(session, group, stage, key, data) {
        const serverName = await this.getSessionStageKeyConnection(session, stage, key);
        await this.sendToServer(serverName, session, group, stage, key, data);
        return serverName;
    }

    async sendToServer(serverName, session, group, stage, key, data) {
        const connection = this._connectionService.getConnection(serverName);
        const raw = serializeData({ session, group, stage, key, data });
        return connection.sendData(raw + "\n");
    }

    getSessionStageKeyServer(session, stage, key) {
        const hash = getHash(session, stage, key);
        return this._sessionStageKeyMap[hash];
    }

    setSessionStageKeyServer(session, stage, key, server) {
        const hash = getHash(session, stage, key);
        this._sessionStageKeyMap[hash] = server;
    }

    async askSessionStageKeyServer(session, stage, key) {
        return await this.ask("getSessionStageKeyServer", {
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
            .forEach(connection => {
                promises.push(connection.notify(type, data));
            });
        await Promise.all(promises);
    }

    async ask(type, data) {
        const promises = [];
        this._connectionService
            .getConnections()
            .forEach(connection => {
                promises.push(connection.ask(type, data));
            });
        return await this.raceData(promises);
    }

    shuffle(a) {
        for (let i = a.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [a[i], a[j]] = [a[j], a[i]];
        }
        return a;
    }

    getRandomSortedAliveConnections(stage) {
        return this.shuffle(
            this._connectionService
            .getConnections()
            .filter((server) => {
                return server.isContainsStage(stage) && server.isAlive();
            })
            .map((server) => server.getName())
        );
    }

    async getRandomAliveConnection(stage) {
        const sorted = this.getRandomSortedAliveConnections(stage);

        if (sorted.length === 0) {
            throw Error("There are no alive servers");
        }

        return sorted[0];
    }
}
