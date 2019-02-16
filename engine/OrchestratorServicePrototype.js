const SendWritableStream = require("./SendWritableStream.js");
const EventStreamTransformStream = require("./EventStreamTransformStream.js");
const {
    serializeData,
    getHash,
} = require("./SerializationUtil.js");

module.exports = class OrchestratorServicePrototype {

    constructor(options) {
        this._connectionService = options.connectionService;
        this._preferableServerName = options.preferableServerName;
        this._connectionStageMap = {};
        this._stageKeyMap = {};
    }

    getLinesStream() {
        return new EventStreamTransformStream();
    }

    errorProcessing(err) {
        if (err) {
            console.error("Pipeline failed.", err);
        }
    }

    getOutcomeStream(session, stage, key) {
        return new SendWritableStream({
            send: this.push.bind(this),
            serverName: this._preferableServerName,
            session, stage, key,
        });
    }

    getServerStageKeyCount(serverStageHash) {
        return this._connectionStageMap[serverStageHash] || 0;
    }

    getServerStageKeySorted(serverName, stage) {
        return this._connectionService
            .getConnections()
            .filter((server) => {
                return server.isContainsStage(stage) && server.isAlive();
            })
            .sort((a, b) => {
                const aServerHash = getHash(a.getName(), stage);
                const aKeyCount = getServerStageKeyCount(aServerHash);
                const bServerHash = getHash(b.getName(), stage);
                const bKeyCount = getServerStageKeyCount(bServerHash);
                if (aKeyCount === bKeyCount) {
                    if (a.getName() === serverName) {
                        return 1;
                    } else if (b.getName() === serverName) {
                        return -1;
                    }
                } else {
                    return aKeyCount < bKeyCount ? 1 : -1;
                }
            })
            .map((server) => server.getName());
    }

    async getNextServer(preferableServerName, stage, key) {
        const stageKeyHash = getHash(stage, key);
        if (
            key != null && this._stageKeyMap[stageKeyHash]
        ) {
            return this._stageKeyMap[stageKeyHash];
        } else {
            const sorted = this.getServerStageKeySorted(preferableServerName, stage);

            if (sorted.length === 0) {
                throw Error("There are no alive servers");
            }

            return sorted[0] || preferableServerName;
        }
    }

    setNextServer(serverName, stage, key) {
        if (key != null) {
            const stageKeyHash = getHash(stage, key);
            this._stageKeyMap[stageKeyHash] = serverName;
        }

        const serverStageHash = getHash(serverName, stage);

        if (!this._connectionStageMap[serverStageHash]) {
            this._connectionStageMap[serverStageHash] = 1;
        } else {
            this._connectionStageMap[serverStageHash]++;
        }
    }

    async push(preferableServerName, session, stage, key, data) {
        const nextServerName = await this.getNextServer(preferableServerName, stage, key);
        this.setNextServer(nextServerName, stage, key);
        const server = this._connectionService.getConnection(nextServerName);
        const raw = serializeData({ session, stage, key, data });
        server.sendData(raw + "\n");
    }
}
