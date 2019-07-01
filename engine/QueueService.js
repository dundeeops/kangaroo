const fs = require("fs");
const path = require("path");
const {
    getId,
} = require("./SerializationUtil.js");

const DIR = path.resolve("./queue");

const defaultOptions = {
    dir: DIR,
    memoryLimit: 10000,
};

module.exports = class QueueService {
    constructor(_options = {}) {
        const options = {
            ...defaultOptions,
            ..._options,
        };
        this._dir = options.dir;
        this._memoryLimit = options.memoryLimit;

        this.init(options);
    }

    async init() {
        this._memoryQueue = [];
        this._fileList = [];
    }

    async initCache() {
        await this.initCacheFolder();
        await this.readCacheFolder();
    }

    async initCacheFolder() {
        await await new Promise(r => fs.exists(this._dir, (res, error) => {
            if (error) {
                return e(error);
            }
            if (!res) {
                fs.mkdir(this._dir, r);
            } else {
                r();
            }
        }));
    }

    async readCacheFolder() {
        await await new Promise((r, e) => fs.readdir(this._dir, (error, files) => {
            if (error) {
                return e(error);
            }
            files.forEach((file) => {
                this._fileList.push(
                    path.resolve(this._dir, file),
                );
            });
            r();
        }));
    }
    
    async unlinkFile(file) {
        await new Promise((r, e) => fs.unlink(
            file,
            (error) => {
                if (error) {
                    return e(error);
                }
                r();
            },
        ));
    }

    async popDataStorage() {
        const file = this._fileList.pop();
        if (file) {
            return await new Promise((r, e) => {
                fs.readFile(file, { encoding: "utf8" }, async (error, data) => {
                    if (error) {
                        return e(error);
                    }
                    await this.unlinkFile(file);
                    r(data.split('\n'));
                });
            });
        } else {
            return [];
        }
    }

    async pushToDiskStorage(data) {
        const name = getId();
        const file = path.resolve(this._dir, name);
        await new Promise((r, e) => fs.appendFile(
            file,
            data.join('\n'),
            (error) => {
                if (error) {
                    return e(error);
                }
                r();
            },
        ));
        this._fileList.push(file);
    }

    async preserve() {
        if (this._memoryQueue.length) {
            await this.pushToDiskStorage(this._memoryQueue);
            this._memoryQueue = [];
        }
    }

    async push(data) {
        if (this._memoryQueue.length > this._memoryLimit) {
            await this.pushToDiskStorage(this._memoryQueue);
            this._memoryQueue = [];
        }
        this._memoryQueue.push(data);
    }

    async pop() {
        if (this._memoryQueue.length === 0 && this._fileList.length > 0) {
            this._memoryQueue = await this.popDataStorage();
        }
        return this._memoryQueue.pop();
    }

    async popLastN(count) {
        const result = [];
        for (const i = 0; i < count; i++) {
            result.push(await this.pop());
        }
        return result.filter(s => !!s);
    }
}
