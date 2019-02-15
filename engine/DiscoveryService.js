const { Etcd3 } = require("etcd3");
// bluebird.promisifyAll(Etcd.prototype);
const {
    serializeData,
    deserializeData,
} = require("./SerializationUtil.js");

const DIR = "/noremap";

module.exports = class DiscoveryService {

    constructor(options) {
        this._connection = null;
        this._dir = options.dir || DIR;
        this._etcd = new Etcd3(options);
    }

    getKey(key) {
        return this._dir + "/" + key;
    }

    getPath(...args) {
        return args.join("/");
    }

    getStreamServerPath(session, stage, key) {
        return this.getPath("streams", session, stage, key, "server");
    }

    getValue(result) {
        return result.node.value;
    }

    async getStreamServer(session, stage, key) {
        const path = this.getStreamServerPath(session, stage, key);
        return this.getValue(await this._get(this.getKey(path)));
    }

    async setStreamServer(session, stage, key, server) {
        const path = this.getStreamServerPath(session, stage, key);
        this._set(this.getKey(path), server);
    }

    async getServers() {
        return deserializeData(
            this.getValue(
                await this._get(this.getKey("servers")),
            ),
        );
    }

    async registerServer(server) {
        const servers = await this.getServers();
        servers.push(server);
        await this._set(this.getKey("servers"), servers);
    }

    async removeServer(server) {
        const servers = await this.getServers();
        const index = servers.indexOf(server);
        if (index > -1) {
            servers.splice(index, 1);
        }
        await this._set(this.getKey("servers"), serializeData(servers));
    }

    async test() {
        await this._etcd.put('foo').value('bar');
        console.log('foo is:', await this._etcd.get('foo').string());
        // await this._set(this.getKey("servers/test"), "testtest");
        // await this._set(this.getKey("servers/abc"), "testtest");
        // console.log(await this._get(this.getKey("servers")));
        // await this.clean("servers");
    }

    async clean(path) {
        await this._rmdir(this.getPath(path), { recursive: true });
    }
}
