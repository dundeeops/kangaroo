const Etcd = require("node-etcd");
const {
    promisify,
} = require("./PromisifyUtil.js");

// TODO: Separate from Etcd.prototype
promisify(Etcd.prototype, ["set", "get", "del", "rmdir"]);

const DIR = "/noremap";
const SERVERS_KEY = "servers";
const STREAMS_KEY = "streams";

const defaultOptions = {
    dir: DIR,
}

// TODO: Implement watch new or removed servers and an example
module.exports = class DiscoveryService {
    constructor(_options) {
        const options = {
            ...defaultOptions,
            ..._options,
        };
        this._dir = options.dir;

        this.init(options);
        this.initEtcd(options);
    }

    init() {
        this._connection = null;
    }

    initEtcd(options) {
        this._etcd = new Etcd(options.hosts, options);
    }

    getKey(key) {
        return this._dir + "/" + key;
    }

    getPath(...args) {
        return this.getKey(args.join("/"));
    }

    async getStreamServer(session, stage, key) {
        try {
            const path = this.getPath(STREAMS_KEY, session, stage, key);
            return await _etcd.getAsync(path).node.value;
        } catch (error) {
            this.checkNotFoundKeyError(error);
            return null;
        }
    }

    async setStreamServer(session, stage, key, server) {
        const path = this.getPath(STREAMS_KEY, session, stage, key);
        this._etcd.setAsync(path, server);
    }

    async getServers() {
        try {
            const nodes = (
                await this._etcd.getAsync(this.getPath(SERVERS_KEY), { dir: true })
            ).node.nodes || [];
            return nodes.map((node) => node.value);
        } catch (error) {
            this.checkNotFoundKeyError(error);
            return [];
        }
    }

    async registerServer(server) {
        await this._etcd.setAsync(this.getPath(SERVERS_KEY, server), server);
    }

    async removeServer(server) {
        try {
            await this._etcd.delAsync(this.getPath(SERVERS_KEY, server));
        } catch (error) {
            this.checkNotFoundKeyError(error);
        }
    }

    checkNotFoundKeyError(error) {
        if (error.errorCode !== 100) {
            throw error;
        }
    }

    async cleanServers() {
        await this.clean(this.getPath(SERVERS_KEY));
    }

    async clean(path) {
        try {
            this._etcd.rmdirAsync(path, { recursive: true });
        } catch (error) {
            this.checkNotFoundKeyError(error);
        }
    }

    async deleteRecursive(node) {
        if (node.nodes) {
            for (let n of node.nodes) {
                await this.delete(n);
            }
        }
        await this._etcd.delAsync(node.key, { dir: node.dir, recursive: node.dir });
    }
}
