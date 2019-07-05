import fs = require("fs");
import path = require("path");
import {
    getId,
} from "./serializationUtil";
import {
  getPromise,
} from "./promiseUtil";

const DIR = path.resolve("./queue");

const defaultOptions = {
    dir: DIR,
    memoryLimit: 10000,
};

export class QueueService {
    _memoryQueue: string[] = [];
    _fileList: string[] = [];
    _promise: Promise<any> = null;
    _resolve: Function = null;
    _dir: string;
    _memoryLimit: number;
    _isInitialized: boolean;

    constructor(_options: {
        dir?: string;
        memoryLimit?: number;
    } = {}) {
        const options = {
            ...defaultOptions,
            ..._options,
        };
        this._dir = options.dir;
        this._memoryLimit = options.memoryLimit;
        this._isInitialized = false;

        this.init();
    }

    async init() {
        this._memoryQueue = [];
        this._fileList = [];
        this._promise = null;
        this._resolve = null;
    }

    async initCache() {
        if (!this._isInitialized) {
            await this.initCacheFolder();
            await this.readCacheFolder();
            this._isInitialized = true;
        }
    }

    async initCacheFolder() {
        await await new Promise((r, e) => fs.exists(this._dir, (res) => {
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
            return await new Promise<string[]>((r, e) => {
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
        await this.initCache();
        if (this._memoryQueue.length) {
            await this.pushToDiskStorage(this._memoryQueue);
            this._memoryQueue = [];
        }
        if (this._resolve) {
            this._resolve();
        }
    }

    async push(data) {
        await this.initCache();
        if (this._memoryQueue.length > this._memoryLimit) {
            await this.pushToDiskStorage(this._memoryQueue);
            this._memoryQueue = [];
        }
        this._memoryQueue.push(data);
        if (this._resolve) {
            this._resolve();
            this._resolve = null;
        }
    }

    async pop() {
        await this.initCache();
        if (this._memoryQueue.length === 0 && this._fileList.length > 0) {
            this._memoryQueue = await this.popDataStorage();
        }
        return this._memoryQueue.pop();
    }

    async popWait() {
        let data = await this.pop();
        if (!data) {
            [this._promise, this._resolve] = getPromise();
            await this._promise;
            data = await this.pop();
        }
        return data;
    }

    async popLastN(count) {
        const result = [];
        for (let i = 0; i < count; i++) {
            result.push(await this.pop());
        }
        return result.filter(s => !!s);
    }
}
