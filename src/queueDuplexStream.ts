import { Writable } from "stream";

import { QueueService } from "./queueService";
import { getPromise } from "./promiseUtil";

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

  async function doNextTask(data) {
    await fn(data);
    inProcessCount--;
    locker[1]();
  }

  async function startCalculations() {
    while (!isStopped) {
      await locker[0];
      let data = await queue.popWait();
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
      await queue.initCache();
      await queue.push(line.toString());
      if (isStopped) {
        isStopped = false;
        startCalculations();
      }
      next();
    },
    async final(done) {
      isStopped = true;
      await queue.preserve();
      done();
    }
  });
};