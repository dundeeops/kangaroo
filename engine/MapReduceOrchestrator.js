const crypto = require("crypto");
const SendToProcessWritable = require("./SendToProcessWritable.js");
const StringToLinesTransform = require("./StringToLinesTransform.js");
const {
    serializeData,
} = require("./Serialization.js");

const separator = ":_:";

module.exports = class MapReduceOrchestrator {

    constructor(options) {
        this._serverPool = options.serverPool;
        this._preferableServerName = options.preferableServerName;
        this._serverStageMap = {};
        this._stageKeyMap = {};
    }

    getLinesStream() {
        return new StringToLinesTransform();
    }

    getHash(...args) {
        return crypto.createHash('sha1').update(args.join(separator)).digest('base64');
    }

    getId() {
        return this.getHash(Math.random().toString())
    }

    errorProcessing(err) {
        if (err) {
            console.error("Pipeline failed.", err);
        }
    }

    getOutcomeStream(session, stage, key) {
        return new SendToProcessWritable({
            send: this.push.bind(this),
            serverName: this._preferableServerName,
            session, stage, key,
        });
    }

    getServerStageKeyCount(serverStageHash) {
        return this._serverStageMap[serverStageHash] || 0;
    }

    getServerStageKeySorted(serverName, stage) {
        return this._serverPool
            .getServers()
            .filter((server) => {
                return server.isContainsStage(stage) && server.isAlive();
            })
            .sort((a, b) => {
                const aServerHash = this.getHash(a.getName(), stage);
                const aKeyCount = getServerStageKeyCount(aServerHash);
                const bServerHash = this.getHash(b.getName(), stage);
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
        const stageKeyHash = this.getHash(stage, key);
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
            const stageKeyHash = this.getHash(stage, key);
            this._stageKeyMap[stageKeyHash] = serverName;
        }

        const serverStageHash = this.getHash(serverName, stage);

        if (!this._serverStageMap[serverStageHash]) {
            this._serverStageMap[serverStageHash] = 1;
        } else {
            this._serverStageMap[serverStageHash]++;
        }
    }

    async push(preferableServerName, session, stage, key, data) {
        const nextServerName = await this.getNextServer(preferableServerName, stage, key);
        this.setNextServer(nextServerName, stage, key);
        const server = this._serverPool.getServer(nextServerName);
        const raw = serializeData({ session, stage, key, data });
        server.sendData(raw + "\n");
    }
}
