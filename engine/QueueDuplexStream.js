const stream = require("stream");
const QueueService = require("./QueueService.js");
const {
  getPromise,
} = require("./PromiseUtil.js");

module.exports = (fn = async (data) => {}, concurrency = 3, readableStream, options = {}) => {
  const queue = new QueueService(options);
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
    await locker;
    if (!isStopped) {
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
      setImmediate(startCalculations, 0);
    }
  }

  return new stream.Writable({
    async write(line, encoding, next) {
      readableStream.pause();
      await queue.push(line);
      readableStream.resume();
      if (isStopped) {
        isStopped = false;
        startCalculations();
      }
      next();
    },
  
    final(done) {
      isStopped = true;
      queue.preserve();
      done();
    }
  });
};