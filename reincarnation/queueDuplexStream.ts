import { Writable, Readable } from "stream";

import { QueueService } from "./QueueService";
import { getPromise, } from "./promiseUtil";

export const queueDuplexStream = ({
  fn,
  concurrency,
  dir,
  memoryLimit,
}: {
  fn: (line: string) => Promise<void>,
  concurrency: number,
  dir: string;
  memoryLimit: number;
}) => {
  const queue = new QueueService({
    dir,
    memoryLimit,
  });
  let inProcessCount = 0;
  let isStopped = true;
  let locker = getPromise();
  locker[1]();
  locker[1] = undefined;

  async function doNextTask(data) {
    await fn(data);
    inProcessCount--;
    if (locker[1]) {
      locker[1]();
      locker[1] = undefined;
    }
  }

  async function startCalculations() {
    while (!isStopped) {
      await locker[0];
      let data = await queue.pop();
      if (!data) {
        data = await queue.popWait();
      }
      if (data) {
        inProcessCount++;
        if (inProcessCount >= concurrency) {
          locker = getPromise();
        }
        doNextTask(data);
      }
    }
  }

  return new Writable({
    async write(line, encoding, next) {
      await queue.push(line);
      next();
      if (isStopped) {
        isStopped = false;
        startCalculations();
      }
    },
  
    async final(done) {
      isStopped = true;
      await queue.preserve();
      done();
    }
  });
};