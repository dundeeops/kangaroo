import { of, from, timer, Observable, throwError, combineLatest } from "rxjs";
import { map, tap, retryWhen, mergeMap, concatMap, catchError, expand, distinct } from "rxjs/operators";
import net from "net";
import domain from "domain";
import { Readable, Writable } from "stream";
import { SplitTransformStream } from "./splitTransformStream";
import { getHash, getId } from "./serializationUtil";
import { queueDuplexStream } from "./queueDuplexStream";
import { getPromise } from "./promiseUtil";

interface IDataBase {
  [key: string]: string | number | boolean | IDataBase | Array<string | number | boolean | IDataBase>;
}

type IData = string | number | boolean | IDataBase | Array<string | number | boolean | IDataBase>;

type IStageFunctionOnData = (data: { stage: string, key?: string, data: IData; }) => Promise<void>;

type IStageFunction = [
  IStageFunctionOnData,
  () => void
] | IStageFunctionOnData

type ISendFn = (stage: string, key: string, data: IData) => Promise<void>;

type IStage = (key: string, send: ISendFn) => Promise<IStageFunction>

interface IStages {
  [key: string]: IStage;
}

interface IProcessingStorage {
  onData: (data: { stage: string, key?: string, data: IData; }) => Promise<void>;
  onFinish: () => Promise<void>
}

interface IProcessing {
  totalSum?: number;
  processed: number;
  processes: number;
  storage: Map<string, IProcessingStorage>;
  usedGroupsTotals: Map<string, number>,
}

type IMachineState = Map<string, IProcessing>;

interface IMachineEventListener {
  onAsk?: (question: QuestionTypeEnum, data: IData) => Promise<IData>;
  onNotify?: (type: NotificationTypeEnum, data: IData) => Promise<void>;
  onData?: (data: ISendData) => Promise<void>;
}

interface IConfiguration {
  connections: {
    key: string;
    manager: {
      hostname: string;
      port: number;
      maxRetryAttempts?: number;
      scalingDuration?: number;
      sleepTimeout?: number;
    };
    worker: {
      hostname: string;
      port: number;
      maxRetryAttempts?: number;
      scalingDuration?: number;
      sleepTimeout?: number;
    };
  }[],
  server?: {
    key: string;
    manager: {
      hostname: string;
      port: number;
      queueDir: string;
      queueLimit?: number;
      concurrency?: number;
      maxRetryAttempts?: number;
      scalingDuration?: number;
    },
    worker: {
      hostname: string;
      port: number;
      queueDir: string;
      queueLimit?: number;
      concurrency?: number;
      maxRetryAttempts?: number;
      scalingDuration?: number;
    },
    stages: IStages;
  },
}

interface IConnectionCache {
  date: number;
  connections: string[];
}

interface IConnectionCachePromise {
  promise: Promise<void>;
  resolve: Function;
}

interface ISessionStageKeyCache {
  connectionKey: string;
}

interface ISessionStageKeyCachePromise {
  promise: Promise<void>;
  resolve: Function;
}

interface IKangarooState {
  machineState: IMachineState;
  machineEventListener: IMachineEventListener;
  sessionStageKeyCache: Map<string, ISessionStageKeyCache | ISessionStageKeyCachePromise>;
  connectionCache: Map<string, IConnectionCache | IConnectionCachePromise>;
  answerResolverState: Map<string, {
    promise: Promise<void>;
    resolve: Function;
  }>;
}

interface ISendData extends IDataBase {
  session: string;
  group: string;
  stage: string;
  key?: string;
  data: IData;
}

enum QuestionTypeEnum {
  GET_STAGE_SERVER = "getStageServer",
  GET_SESSION_STAGE_KEY_SERVER = "getSessionStageKeyServer",
  COUNT_PROCESSED = "countProcessed",
  CAN_GET_STAGE = "canGetStage",
}

enum NotificationTypeEnum {
  END_PROCESSING = "endProcessing",
  NULL_ACHIEVED = "nullAchieved",
}

function makeServer({
  key,
  port,
  hostname,
  queueDir,
  concurrency,
  queueLimit,
  onData,
  onDataError,
  onConnect,
  onError,
  onClose,
}: {
  key: string;
  port: number;
  hostname: string;
  queueDir: string;
  queueLimit?: number;
  concurrency?: number;
  onData: (key: string, socket: net.Socket, data: IData) => Promise<void>;
  onDataError: (key: string, socket: net.Socket, data: string, error: Error) => Promise<void>;
  onConnect: Function;
  onError: Function;
  onClose: Function;
}): net.Server {
  const server = net.createServer(socket => {
    socket
      .pipe(new SplitTransformStream())
      .pipe(queueDuplexStream({
        concurrency: concurrency || 10,
        dir: queueDir,
        fn: async (line) => {
          try {
            await onData(key, socket, JSON.parse(line));
          } catch (error) {
            await onDataError(key, socket, line, error);
          }
        },
        memoryLimit: queueLimit || 1000,
      }))
  });
  server.on("close", () => onClose());
  server.on("error", (error) => onError(error));
  server.on("listening", () => onConnect());
  server.listen(port, hostname);
  return server;
}

function makeConnection({
  key,
  port,
  hostname,
  onData,
  onDataError,
  onConnect,
  onError,
  onClose,
}: {
  key: string;
  port: number;
  hostname: string;
  onData: (key: string, socket: net.Socket, data: IData) => Promise<void>;
  onDataError: (key: string, socket: net.Socket, data: string, error: Error) => Promise<void>;
  onConnect: Function;
  onError: Function;
  onClose: Function;
}) {
  const socket = new net.Socket();
  socket
    .pipe(new SplitTransformStream({
      skipEmpty: true,
    }))
    .pipe(new Writable({
      async write(data, encoding, cb) {
        if (data) {
          try {
            await onData(key, socket, JSON.parse(data.toString()));
          } catch (error) {
            await onDataError(key, socket, data.toString(), error);
          }
        }
        cb();
      },
      final(cb) {
        cb();
      }
    }));
  socket.on("close", () => onClose());
  socket.on("ready", () => onConnect());
  socket.on("error", (error) => onError(error));
  socket.connect({
    host: hostname,
    port: port,
  });
  return socket;
}

async function runServer({
  key,
  port,
  hostname,
  queueDir,
  queueLimit,
  concurrency,
  onData,
  onDataError,
}: {
  key: string;
  port: number;
  hostname: string;
  queueDir: string;
  queueLimit?: number;
  concurrency?: number;
  onData: (key: string, socket: net.Socket, data: IData) => Promise<void>;
  onDataError: (key: string, socket: net.Socket, data: string, error: Error) => Promise<void>;
}): Promise<net.Server> {
  return await new Promise<net.Server>((r, e) => {
    const server = makeServer({
      key,
      port,
      hostname,
      queueDir,
      queueLimit,
      concurrency,
      onData,
      onDataError,
      onConnect() {
        r(server);
      },
      onError(error) {
        e(error);
      },
      onClose() {
        e();
      },
    });
  });
}

async function runConnection({
  key,
  port,
  hostname,
  onData,
  onDataError,
}: {
  key: string;
  port: number;
  hostname: string;
  onData: (key: string, socket: net.Socket, data: IData) => Promise<void>;
  onDataError: (key: string, socket: net.Socket, data: string, error: Error) => Promise<void>;
}): Promise<net.Socket> {
  return new Promise((r, e) => {
    const connection = makeConnection({
      key,
      port,
      hostname,
      onData,
      onDataError,
      onConnect() {
        r(connection);
      },
      onError(error) {
        e(error);
      },
      onClose() {
        e();
      },
    });
  });
}

export const retryStrategy = <T>({
  maxRetryAttempts = 3,
  scalingDuration = 1000,
  message = (retryAttempt, retryDuration) => `Attempt ${retryAttempt}: retrying in ${retryDuration}ms`
}: {
  maxRetryAttempts?: number,
  scalingDuration?: number,
  message?: (retryAttempt: number, retryDuration: number) => string;
} = {}) => (attempts: Observable<T>) => attempts.pipe(
  mergeMap((error, attemptCount) => {
    const retryAttempt = attemptCount + 1;
    console.error(error);
    if (retryAttempt > maxRetryAttempts) {
      return throwError(error);
    }
    console.error(
      message
        ? message(retryAttempt, retryAttempt * scalingDuration)
        : `Attempt ${retryAttempt}: retrying in ${retryAttempt * scalingDuration}ms`
    );
    return timer(retryAttempt * scalingDuration);
  }),
);

function runDomain$<T>({
  run,
  message,
  maxRetryAttempts,
  scalingDuration,
}: {
  run: () => Promise<T>;
  message?: (retryAttempt: number, retryDuration: number) => string;
  maxRetryAttempts?: number;
  scalingDuration?: number;
}) {
  return of(domain.create()).pipe(
    tap(scope => scope.on("error", error => {
      throw error;
    })),
    mergeMap(scope => from(new Promise<T>((r) => {
      scope.run(async () => {
        const result = await run();
        r(result);
      });
    })).pipe(
      retryWhen(
        retryStrategy<T>({
          maxRetryAttempts: maxRetryAttempts || 3,
          scalingDuration: scalingDuration || 1000,
          message,
        }),
      ),
    )),
  );
}

function runDomainServer$({
  key,
  port,
  hostname,
  queueDir,
  queueLimit,
  concurrency,
  onData,
  onDataError,
  maxRetryAttempts,
  scalingDuration,
}: {
  key: string;
  port: number;
  hostname: string;
  queueDir: string;
  queueLimit?: number;
  concurrency?: number;
  onData: (key: string, socket: net.Socket, data: IData) => Promise<void>;
  onDataError: (key: string, socket: net.Socket, data: string, error: Error) => Promise<void>;
  maxRetryAttempts?: number;
  scalingDuration?: number;
}) {
  return runDomain$({
    run: async () => await runServer({
      key,
      hostname,
      port,
      queueDir,
      queueLimit,
      concurrency,
      onData,
      onDataError,
    }),
    message: (retryAttempt, retryDuration) => `[Server ${key} - ${hostname}:${port}] Attempt ${retryAttempt}: retrying to reconnect in ${retryDuration}ms`,
    maxRetryAttempts,
    scalingDuration,
  });
}

function runServer$({
  key,
  port,
  hostname,
  queueDir,
  queueLimit,
  concurrency,
  onData,
  onDataError,
  maxRetryAttempts,
  scalingDuration,
}: {
  key: string;
  port: number;
  hostname: string;
  queueDir: string;
  queueLimit?: number;
  concurrency?: number;
  onData: (key: string, socket: net.Socket, data: IData) => Promise<void>;
  onDataError: (key: string, socket: net.Socket, data: string, error: Error) => Promise<void>;
  maxRetryAttempts?: number;
  scalingDuration?: number;
}) {
  return runDomainServer$({
    key,
    hostname,
    port,
    queueDir,
    queueLimit,
    concurrency,
    onData,
    onDataError,
    maxRetryAttempts,
    scalingDuration,
  }).pipe(
    distinct(),
  );
}

function runConnection$({
  key,
  port,
  hostname,
  onData,
  onDataError,
  maxRetryAttempts,
  scalingDuration,
  sleepTimeout,
}: {
  key: string;
  port: number;
  hostname: string;
  onData: (key: string, socket: net.Socket, data: IData) => Promise<void>;
  onDataError: (key: string, socket: net.Socket, data: string, error: Error) => Promise<void>;
  maxRetryAttempts?: number;
  scalingDuration?: number;
  sleepTimeout?: number;
}) {
  const connection$ = from(
    runConnection({
      key,
      port,
      hostname,
      onData,
      onDataError,
    }),
  ).pipe(
    retryWhen(
      retryStrategy({
        maxRetryAttempts: maxRetryAttempts || 3,
        scalingDuration: scalingDuration || 1000,
        message: (retryAttempt, retryDuration) => `[Connection ${key} - ${hostname}:${port}] Attempt ${retryAttempt}: retrying to reconnect in ${retryDuration}ms`
      }),
    ),
    catchError(() => {
      return of<net.Socket>(null);
    }),
  );

  return connection$.pipe(
    expand(connection => {
      if (!!connection) {
        return from(new Promise<net.Socket>(() => connection)); // Fix for NodeJS < 12.x.x instead of of(...)
      } else {
        return timer(sleepTimeout || 10000).pipe(
          concatMap(() => connection$),
        )
      }
    }),
    distinct(),
  )
}

function runPairConnection$({
  key,
  worker,
  manager,
}: {
  key: string;
  worker: {
    port: number;
    hostname: string;
    onData: (key: string, socket: net.Socket, data: IData) => Promise<void>;
    onDataError: (key: string, socket: net.Socket, data: string, error: Error) => Promise<void>;
    maxRetryAttempts?: number;
    scalingDuration?: number;
    sleepTimeout?: number;
  };
  manager: {
    port: number;
    hostname: string;
    onData: (key: string, socket: net.Socket, data: IData) => Promise<void>;
    onDataError: (key: string, socket: net.Socket, data: string, error: Error) => Promise<void>;
    maxRetryAttempts?: number;
    scalingDuration?: number;
    sleepTimeout?: number;
  }
}) {
  return combineLatest([
    of(key),
    runConnection$({ key, ...manager }),
    runConnection$({ key, ...worker }),
  ]);
}

function runPairServer$({
  key,
  worker,
  manager,
  onDataManager,
  onDataManagerError,
  onDataWorker,
  onDataWorkerError,
}: {
  key: string;
  onDataManager: (key: string, socket: net.Socket, data: IData) => Promise<void>;
  onDataManagerError: (key: string, socket: net.Socket, data: string, error: Error) => Promise<void>;
  onDataWorker: (key: string, socket: net.Socket, data: IData) => Promise<void>;
  onDataWorkerError: (key: string, socket: net.Socket, data: string, error: Error) => Promise<void>;
  worker: {
    port: number;
    hostname: string;
    queueDir: string;
    queueLimit?: number;
    concurrency?: number;
    maxRetryAttempts?: number;
    scalingDuration?: number;
  };
  manager: {
    port: number;
    hostname: string;
    queueDir: string;
    queueLimit?: number;
    concurrency?: number;
    maxRetryAttempts?: number;
    scalingDuration?: number;
  }
}) {
  return combineLatest([
    runServer$({
      key,
      onData: onDataManager,
      onDataError: onDataManagerError,
      ...manager,
    }),
    runServer$({
      key,
      onData: onDataWorker,
      onDataError: onDataWorkerError,
      ...worker,
    }),
  ]);
}

function runConnections$({
  onDataConnectionManager,
  onDataConnectionManagerError,
  onDataConnectionWorker,
  onDataConnectionWorkerError,
  connections,
}: {
  onDataConnectionManager: (key: string, socket: net.Socket, data: IData) => Promise<void>;
  onDataConnectionManagerError: (key: string, socket: net.Socket, data: string, error: Error) => Promise<void>;
  onDataConnectionWorker: (key: string, socket: net.Socket, data: IData) => Promise<void>;
  onDataConnectionWorkerError: (key: string, socket: net.Socket, data: string, error: Error) => Promise<void>;
  connections: {
    key: string;
    manager: {
      port: number;
      hostname: string;
      maxRetryAttempts?: number;
      scalingDuration?: number;
      sleepTimeout?: number;
    };
    worker: {
      port: number;
      hostname: string;
      maxRetryAttempts?: number;
      scalingDuration?: number;
      sleepTimeout?: number;
    };
  }[];
}) {
  return combineLatest(
    connections.map(config => runPairConnection$({
      key: config.key,
      manager: {
        ...config.manager,
        onData: onDataConnectionManager,
        onDataError: onDataConnectionManagerError,
      },
      worker: {
        ...config.worker,
        onData: onDataConnectionWorker,
        onDataError: onDataConnectionWorkerError,
      },
    }))
  ).pipe(
    map(connections => connections.filter(
      ([key, manager, worker]) => !!manager && !!worker),
    ),
  );
}

function getDefaultProcessingMap(): IProcessing {
  return {
    totalSum: null,
    processed: 0,
    processes: 0,
    storage: new Map(),
    usedGroupsTotals: new Map(),
  }
}

async function write(socket: net.Socket, data: IData) {
  return await new Promise<void>((r, e) => socket.write(
    JSON.stringify(data) + '\n',
    'utf8',
    error => {
      if (error) {
        return e(error);
      }
      r();
    },
  ));
}

export function runMachine$(configuration: IConfiguration) {
  return of<IKangarooState>({
    machineState: new Map(),
    machineEventListener: {},
    answerResolverState: new Map(),
    sessionStageKeyCache: new Map(),
    connectionCache: new Map(),
  }).pipe(
    mergeMap(state => combineLatest([
      of(state),
      configuration.server ? runPairServer$({
        key: configuration.server.key,
        manager: configuration.server.manager,
        worker: configuration.server.worker,
        onDataManager: async (key, socket, raw: IDataBase) => {
          const { id, question, type, data } = raw;
          if (id) {
            const answer = await state.machineEventListener.onAsk(question as QuestionTypeEnum, data);
            return await write(socket, {
              id,
              answer,
            });
          } else {
            await state.machineEventListener.onNotify(type as NotificationTypeEnum, data);
          }
        },
        onDataWorker: async (key, socket, data) => {
          await state.machineEventListener.onData(data as ISendData);
        },
        onDataManagerError: async (key: string, socket: net.Socket, data: string, error: Error) => {
          console.error(key, data, error);
        },
        onDataWorkerError: async (key: string, socket: net.Socket, data: string, error: Error) => {
          console.error(key, data, error);
        },
      }) : of<net.Server>(null),
      runConnections$({
        connections: configuration.connections,
        onDataConnectionManager: async (key, socket, raw: IDataBase) => {
          const { id, answer } = raw;
          state.answerResolverState.get(id as string).resolve(answer);
        },
        onDataConnectionWorker: async (key, socket, data) => { }, // Worker obliged to ignore any incoming messages
        onDataConnectionManagerError: async (key: string, socket: net.Socket, data: string, error: Error) => {
          console.error(key, data, error);
        },
        onDataConnectionWorkerError: async (key: string, socket: net.Socket, data: string, error: Error) => {
          console.error(key, data, error);
        },
      }),
    ])),
    map(([state, server, connections]) => {
      state.machineEventListener.onAsk = async (question, data) => {
        const { session, stage, group, key } = data as {
          session: string;
          group: string;
          stage: string;
          key: string;
        };
        switch (question) {
          case QuestionTypeEnum.CAN_GET_STAGE:
            return Object.keys(configuration.server.stages).includes(stage);
          case QuestionTypeEnum.COUNT_PROCESSED:
            const map = state.machineState.get(group);
            return map ? map.processed : 0;
          case QuestionTypeEnum.GET_SESSION_STAGE_KEY_SERVER:
            const hash = getHash(session, stage, key);
            return state.sessionStageKeyCache.has(hash)
              && (state.sessionStageKeyCache.get(hash) as ISessionStageKeyCache).connectionKey;
          case QuestionTypeEnum.GET_STAGE_SERVER:
            return !!configuration.server.stages[stage];
        }
        return null;
      };

      state.machineEventListener.onNotify = async (type, data) => {
        const { group, totalSum } = data as {
          group: string;
          totalSum: number;
        };
        let map = group && state.machineState.get(group);
        switch (type) {
          case NotificationTypeEnum.NULL_ACHIEVED:
            checkProcessingMap(group);
            map = state.machineState.get(group);
            if (map) {
              map.totalSum = totalSum;
              const processedArray = await askAll(
                QuestionTypeEnum.COUNT_PROCESSED,
                {
                  group,
                },
              );
              const processed = processedArray
                .reduce((value, [key, item]: [string, number]) => value + item, 0);
              if (processed === totalSum) {
                notify(
                  NotificationTypeEnum.END_PROCESSING,
                  {
                    group,
                  },
                );
              }
            }
            return;
          case NotificationTypeEnum.END_PROCESSING:
            if (map) {
              for (const [hash, storage] of map.storage) {
                await storage.onFinish();
              }

              for (const [nextGroup, totalSum] of map.usedGroupsTotals) {
                notify(
                  NotificationTypeEnum.NULL_ACHIEVED,
                  {
                    group: nextGroup,
                    totalSum,
                  },
                );
              }

              state.machineState.delete(group);
            }
            return;
        }
      };

      state.machineEventListener.onData = async ({ session, group, stage, key, data }: ISendData) => {
        const hash = getHash(session, stage, key || getId());
        checkProcessingMap(group);
        const machineState = state.machineState.get(group);
        machineState.processed++;
        machineState.processes++;
        const storage = await getStorage({group, hash, session, stage, key});
        await storage.onData({ stage, key, data });
        if (!key) {
          machineState.storage.delete(hash);
        }
        const totalSum = machineState.totalSum;
        machineState.processes--;
        if (machineState.processes === 0 && totalSum != null) {
          const processedArray = await askAll(
            QuestionTypeEnum.COUNT_PROCESSED,
            {
              group,
            },
          );
          const processed = processedArray
            .reduce((value, [key, count]: [string, number]) => value + count, 0);
          if (processed === totalSum) {
            notify(
              NotificationTypeEnum.END_PROCESSING,
              {
                group,
              },
            );
          }
        }
      };

      async function askAll(question: QuestionTypeEnum, data: IData) {
        const results: [string, IData][] = await Promise.all(connections.map(
          async ([key, manager, worker]): Promise<[string, IData]> => {
            const id = getId();
            const [promise, resolve] = getPromise();
            state.answerResolverState.set(id, {
              promise,
              resolve,
            });
            await write(manager, {
              id,
              question,
              data,
            });
            const answer: IData = await promise;
            state.answerResolverState.delete(id);
            return [key, answer];
          }
        ));
        return results.filter(([key, result]) => !!result);
      }

      async function ask(question: QuestionTypeEnum, data: IData) {
        let resolved = false;
        let count = 0;
        let result = await new Promise<[string, IData]>((r, e) => connections.forEach(
          async ([key, manager, worker]) => {
            const id = getId();
            const [promise, resolve] = getPromise();
            state.answerResolverState.set(id, {
              promise,
              resolve,
            });
            await write(manager, {
              id,
              question,
              data,
            });
            const answer = await promise;
            if (answer && !resolved) {
              resolved = true;
              r([key, answer]);
            }
            state.answerResolverState.delete(id);
            count++;
            if (count === connections.length && !resolved) {
              r(null);
            }
          }
        ));
        return result;
      }

      async function notify(type: NotificationTypeEnum, data: IData) {
        connections.forEach(
          async ([key, manager, worker]) => {
            await write(manager, {
              type,
              data,
            });
          }
        );
      }

      async function askSessionStageKeyServer(session: string, stage: string, key?: string): Promise<string> {
        const hash = getHash(session, stage, key);
        let sessionKeyCache: ISessionStageKeyCache;
        let sessionKeyCachePromise = state.sessionStageKeyCache.get(hash) as ISessionStageKeyCachePromise;
        if (sessionKeyCachePromise && sessionKeyCachePromise.promise) {
          await sessionKeyCachePromise.promise;
          sessionKeyCache = state.sessionStageKeyCache.get(hash) as ISessionStageKeyCache;
        }
        sessionKeyCache = state.sessionStageKeyCache.get(hash) as ISessionStageKeyCache;
        if (!sessionKeyCache) {
          const [promise, resolve] = getPromise();
          state.sessionStageKeyCache.set(hash, { promise, resolve });
          sessionKeyCache = {
            connectionKey: null,
          };
          const answer = await ask(
            QuestionTypeEnum.GET_SESSION_STAGE_KEY_SERVER,
            {
              session,
              stage,
              key,
            },
          );
          sessionKeyCache.connectionKey = answer ? answer[0] : null;
          if (!sessionKeyCache.connectionKey) {
            const answer = await ask(
              QuestionTypeEnum.CAN_GET_STAGE,
              {
                stage,
              },
            );
            sessionKeyCache.connectionKey = answer ? answer[0] : null;
          }
          state.sessionStageKeyCache.set(hash, sessionKeyCache);
          resolve();
        }
        return sessionKeyCache.connectionKey;
      }

      async function findCanGetStageConnections(
        stage: string,
        CAN_GET_STAGE_TIMEOUT = 60000,
        CAN_GET_STAGE_AWAIT_TIMEOUT = 1000,
      ) {
        const startTime = +new Date();
        let connections: string[] = [];
        while (!connections.length && startTime + CAN_GET_STAGE_TIMEOUT > +new Date()) {
          const answerResolverState = await askAll(QuestionTypeEnum.CAN_GET_STAGE, {
            stage,
          });
          connections = answerResolverState.map(([key, result]) => key);
          if (!connections.length) {
            await new Promise(r => setTimeout(r, CAN_GET_STAGE_AWAIT_TIMEOUT));
          }
        }
        return connections;
      }

      async function findStageConnection(stage: string) {
        let connectionCache = state.connectionCache.get(stage) as IConnectionCache;
        const connectionCachePromise = state.connectionCache.get(stage) as IConnectionCachePromise;
        if (connectionCachePromise && connectionCachePromise.promise) {
          await connectionCachePromise.promise;
          connectionCache = state.connectionCache.get(stage) as IConnectionCache;
        }
        if (
          !connectionCache
          || ((+new Date()) - connectionCache.date) > 10 * 1000
          || (connectionCache.connections && !connectionCache.connections.length)
        ) {
          const [promise, resolve] = getPromise();
          state.connectionCache.set(stage, {
            promise,
            resolve,
          });
          const connections = await findCanGetStageConnections(stage);
          connectionCache = {
            date: +new Date(),
            connections,
          };
          state.connectionCache.set(stage, connectionCache);
          resolve();
        }
        if (connectionCache.connections) {
          return connectionCache.connections[
            Math.floor(Math.random() * connectionCache.connections.length)
          ];
        }
      }

      async function getSessionStageKeyConnectionScript(session: string, stage: string, key?: string): Promise<string> {
        let connectionKey: string;

        if (key) {
          connectionKey = await askSessionStageKeyServer(session, stage, key);
        }

        if (!connectionKey) {
          connectionKey = await findStageConnection(stage);
        }

        if (key) {
          const hash = getHash(session, stage, key);
          state.sessionStageKeyCache.set(hash, { connectionKey });
        }

        return connectionKey;
      }

      async function sendToServer(connectionKey: string, data: IData) {
        if (!connectionKey) {
          throw Error("NO CONNECTIONS FOUND TO SEND");
        }
        const connection = connections.find(([key]) => key === connectionKey);
        if (connection) {
          const [key, manager, worker] = connection;
          await write(worker, data);
        } else {
          throw Error(`NO CONNECTION FOUND WITH KEY ${connectionKey}`);
        }
      }

      async function send({ session, group, stage, key, data }: ISendData) {
        const connectionKey = await getSessionStageKeyConnectionScript(
          session,
          stage,
          key,
        );
        const raw = {
          session,
          group,
          stage,
          key,
          data,
        };
        if (configuration.server && configuration.server.key
          && (
            !connectionKey || configuration.server.key === connectionKey
          )
        ) {
          await state.machineEventListener.onData(raw);
        } else {
          await sendToServer(
            connectionKey,
            raw
          );
        }
      }

      function checkProcessingMap(group: string) {
        if (!state.machineState.get(group)) {
          state.machineState.set(group, getDefaultProcessingMap());
        }
      }

      function getMapperScript(stage: string) {
        let mapper = configuration.server.stages[stage];
        return mapper;
      }

      function parseMapperResult(mapResult): [
        (data: { stage: string, key?: string, data: IData; eof: boolean }) => Promise<void>,
        () => Promise<void>
      ] {
        if (Array.isArray(mapResult)) {
          return [mapResult[0], mapResult[1]];
        } else {
          return [mapResult, async () => { }];
        }
      }

      function increaseUsedGroupSend({
        group,
        nextGroup,
      }: {
        group: string;
        nextGroup: string;
      }) {
        const machineState = state.machineState.get(group);
        machineState.usedGroupsTotals.set(
          nextGroup,
          (machineState.usedGroupsTotals.get(nextGroup) || 0) + 1,
        );
      }

      function getSendCatchUsedGroupWrap({
        group,
        session,
      }: {
        group: string;
        session: string;
      }): ISendFn {
        return async (stage: string, key: string, data: IData) => {
          const nextGroup = getHash(group, stage);
          increaseUsedGroupSend({group, nextGroup});
          await send({ session, group: nextGroup, stage, key, data });
        };
      }

      async function buildMap({
        group,
        key,
        session,
        mapper,
      }: {
        group: string;
        session: string;
        key: string;
        mapper: IStage;
      }): Promise<IProcessingStorage> {
        const sendWrap = getSendCatchUsedGroupWrap({group, session});
        const mapResult = await mapper(key, sendWrap);
        const mapCouple = parseMapperResult(mapResult);
        return {
          onData: mapCouple[0],
          onFinish: mapCouple[1],
        };
      }

      async function getStorage({
        group,
        hash,
        key,
        session,
        stage,
      }: {
        group: string;
        hash: string;
        session: string;
        stage: string;
        key?: string;
      }) {
        let storage = state.machineState.get(group).storage.get(hash);
        if (!storage) {
          const mapper = getMapperScript(stage);
          storage = await buildMap({group, session, key, mapper});
          state.machineState.get(group).storage.set(hash, storage);
        }
        return storage;
      }

      function runStream(stage: string, key: string, stream: Readable) {
        const session = getId();
        const group = getHash(session, stage);
        let totalSum = 0;
        return stream
          .pipe(new SplitTransformStream())
          .pipe(new Writable({
            async write(chunk, encoding, callback) {
              totalSum++;
              await send({
                session,
                group,
                stage,
                key,
                data: chunk.toString(),
              });
              callback(null);
            },
            final(cb) {
              notify(
                NotificationTypeEnum.NULL_ACHIEVED,
                {
                  group,
                  totalSum,
                },
              );
              cb();
            }
          }));
      }
      return {
        server,
        connections,
        runStream,
      };
    }),
  );
}
