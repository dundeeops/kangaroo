import fs from 'fs';
import path from 'path';
import {
  getId,
} from './serializationUtil';
import {
  getPromise,
} from './promise';

const defaultOptions = {
  dir: path.resolve('./queue'),
  memoryLimit: 10000,
};

export class QueueService {
  private memoryQueue: string[] = [];
  private fileList: string[] = [];
  private promise: Promise<any> = null;
  private resolve: Function = null;
  private promiseDiskOperation: Promise<any> = null;
  private resolveDiskOperation: Function = null;
  private dir: string;
  private memoryLimit: number;
  private isInitialized: boolean;

  constructor(_options: {
    dir?: string;
    memoryLimit?: number;
  } = {}) {
    const options = {
      ...defaultOptions,
      ..._options,
    };
    this.dir = options.dir;
    this.memoryLimit = options.memoryLimit;
    this.isInitialized = false;

    this.init();
  }

  private init() {
    this.memoryQueue = [];
    this.fileList = [];
    [this.promise, this.resolve] = getPromise();
    this.resolve();
    [this.promiseDiskOperation, this.resolveDiskOperation] = getPromise();
    this.resolveDiskOperation();
  }

  public async initCache() {
    if (!this.isInitialized) {
      this.isInitialized = true;
      await this.initCacheFolder();
      await this.readCacheFolder();
    }
  }

  private async initCacheFolder() {
    await await new Promise((r, e) => fs.exists(this.dir, (res) => {
      if (!res) {
        fs.mkdir(this.dir, r);
      } else {
        r();
      }
    }));
  }

  private async readCacheFolder() {
    await await new Promise((r, e) => fs.readdir(this.dir, (error, files) => {
      if (error) {
        return e(error);
      }
      files.forEach((file) => {
        this.fileList.push(
          path.resolve(this.dir, file),
        );
      });
      r();
    }));
  }

  private async unlinkFile(file: string) {
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

  private async popDataStorage() {
    const file = this.fileList.shift();
    if (file) {
      return await new Promise<string[]>((r, e) => {
        fs.readFile(file, { encoding: 'utf8' }, async (error, data) => {
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

  private async pushToDiskStorage(data: string[]) {
    const name = `${this.fileList.length}_${getId()}`;
    const file = path.resolve(this.dir, name);
    await new Promise((r, e) => fs.writeFile(
      file,
      data.join('\n'),
      (error) => {
        if (error) {
          return e(error);
        }
        r();
      },
    ));
    this.fileList.push(file);
  }

  async preserve() {
    if (this.memoryQueue.length) {
      await this.pushToDiskStorage(this.memoryQueue);
      this.memoryQueue = [];
    }
    this.resolve();
  }

  public async push(data: string) {
    await this.promiseDiskOperation;
    if (this.memoryQueue.length > this.memoryLimit * 2) {
      [this.promiseDiskOperation, this.resolveDiskOperation] = getPromise();
      const arr = this.memoryQueue.slice(this.memoryLimit);
      this.memoryQueue = [data, ...this.memoryQueue.slice(0, this.memoryLimit)];
      await this.pushToDiskStorage(arr);
      this.resolveDiskOperation();
    } else {
      this.memoryQueue.push(data);
    }
    this.resolve();
  }

  public async pop() {
    await this.promiseDiskOperation;
    if (this.memoryQueue.length === 0 && this.fileList.length > 0) {
      [this.promiseDiskOperation, this.resolveDiskOperation] = getPromise();
      this.memoryQueue = await this.popDataStorage();
      this.resolveDiskOperation();
    }
    const data = this.memoryQueue.shift();
    return data;
  }

  public async popWait() {
    let data = await this.pop();
    if (!data) {
      [this.promise, this.resolve] = getPromise();
      await this.promise;
      data = await this.pop();
    }
    return data;
  }

  public async popLastN(count: number) {
    const result = [];
    for (let i = 0; i < count; i++) {
      result.push(await this.pop());
    }
    return result.filter(s => !!s);
  }
}
