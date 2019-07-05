import { of, from, timer, Observable, throwError, combineLatest } from "rxjs";
import { map, tap, switchMap, retryWhen, delayWhen, mergeMap, share, concat, concatMap, find } from "rxjs/operators";
import net from "net";
import domain from "domain";
import path from "path";
import { SplitTransformStream } from "./splitTransformStream";
import { Writable } from "stream";
import { getHash, getId } from "./serializationUtil";
import { queueDuplexStream } from "./queueDuplexStream";
import { getPromise } from "./promiseUtil";

interface IDataBase {
  [key: string]: string | number | boolean | IDataBase | Array<string | number | boolean | IDataBase>;
}

type IData = string | number | boolean | IDataBase | Array<string | number | boolean | IDataBase>;

type IStageFunction = [
  (data: IData) => Promise<void>,
  () => void
] | ((data: IData) => Promise<void>)

type ISendFn = (stage: string, key: string, data: IData) => Promise<void>;

type IStage = (key: string, send: ISendFn) => Promise<IStageFunction>

interface IStages {
  [key: string]: IStage;
}

function makeServer({
  key,
  port,
  hostname,
  queueDir,
  queueLimit,
  onData,
  onConnect,
  onError,
  onClose,
}: {
  key: string;
  port: number;
  hostname: string;
  queueDir: string;
  queueLimit?: number;
  onData: (key: string, socket: net.Socket, data: IData) => Promise<void>;
  onConnect: Function;
  onError: Function;
  onClose: Function;
}): net.Server {
  const server = net.createServer(socket => {
    socket
      .pipe(new SplitTransformStream())
      .pipe(queueDuplexStream({
        readableStream: socket,
        concurrency: 10,
        dir: queueDir,
        fn: async (line) => {
          await onData(key, socket, JSON.parse(line));
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
  onConnect,
  onError,
  onClose,
}: {
  key: string;
  port: number;
  hostname: string;
  onData: (key: string, socket: net.Socket, data: IData) => Promise<void>;
  onConnect: Function;
  onError: Function;
  onClose: Function;
}) {
  const socket = new net.Socket();
  socket
    .pipe(new SplitTransformStream())
    .pipe(new Writable({
      async write(data, encoding, cb) {
        await onData(key, socket, JSON.parse(data.toString()));
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
  onData,
}: {
  key: string;
  port: number;
  hostname: string;
  queueDir: string;
  queueLimit?: number;
  onData: (key: string, socket: net.Socket, data: IData) => Promise<void>;
}): Promise<net.Server> {
  return await new Promise<net.Server>((r, e) => {
    const server = makeServer({
      key,
      port,
      hostname,
      queueDir,
      queueLimit,
      onData,
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
}: {
  key: string;
  port: number;
  hostname: string;
  onData: (key: string, socket: net.Socket, data: IData) => Promise<void>;
}): Promise<net.Socket> {
  return new Promise((r, e) => {
    const connection = makeConnection({
      key,
      port,
      hostname,
      onData,
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

export const retryServerStrategy = <T>({
  maxRetryAttempts = 3,
  scalingDuration = 1000,
}: {
  maxRetryAttempts?: number,
  scalingDuration?: number,
} = {}) => (attempts: Observable<T>) => attempts.pipe(
  mergeMap((error, i) => {
    const retryAttempt = i + 1;
    console.error(error);
    if (retryAttempt > maxRetryAttempts) {
      return throwError(error);
    }
    console.error(
      `Attempt ${retryAttempt}: retrying in ${retryAttempt * scalingDuration}ms`
    );
    return timer(retryAttempt * scalingDuration);
  }),
);

function runDomain$<T>(run: () => Promise<T>) {
  return of(domain.create()).pipe(
    tap(scope => scope.on("error", error => {
      throw error;
    })),
    mergeMap(d => from(new Promise<T>((r) => {
      d.run(async () => {
        const result = await run();
        r(result);
      });
    })).pipe(
      retryWhen(
        retryServerStrategy<T>({
          maxRetryAttempts: 3,
          scalingDuration: 1000,
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
  onData,
}: {
  key: string;
  port: number;
  hostname: string;
  queueDir: string;
  queueLimit?: number;
  onData: (key: string, socket: net.Socket, data: IData) => Promise<void>;
}) {
  return runDomain$(
    async () => await runServer({
      key,
      hostname,
      port,
      queueDir,
      queueLimit,
      onData,
    }),
  );
}

function runServer$({
  key,
  port,
  hostname,
  queueDir,
  queueLimit,
  onData,
}: {
  key: string;
  port: number;
  hostname: string;
  queueDir: string;
  queueLimit?: number;
  onData: (key: string, socket: net.Socket, data: IData) => Promise<void>;
}) {
  return runDomainServer$({
    key,
    hostname,
    port,
    queueDir,
    queueLimit,
    onData,
  });
}

export const retryConnectionStrategy = <T>({
  maxRetryAttempts = 3,
  scalingDuration = 1000,
  timeout = 30000,
}: {
  maxRetryAttempts?: number,
  scalingDuration?: number,
  timeout?: number,
} = {}) => (attempts: Observable<T>) => attempts.pipe(
  mergeMap((error, i) => {
    const retryAttempt = i + 1;
    console.error(error);
    if (retryAttempt % maxRetryAttempts === 0) {
      return timer(timeout);
    }
    console.error(
      `Attempt ${retryAttempt}: retrying to reconnect in ${retryAttempt * scalingDuration}ms`
    );
    return timer(retryAttempt * scalingDuration);
  }),
);

function runConnection$({
  key,
  port,
  hostname,
  onData,
}: {
  key: string;
  port: number;
  hostname: string;
  onData: (key: string, socket: net.Socket, data: IData) => Promise<void>;
}) {
  return from(
    runConnection({
      key,
      port,
      hostname,
      onData,
    }),
  ).pipe(
    retryWhen(
      retryConnectionStrategy({
        maxRetryAttempts: 3,
        scalingDuration: 1000,
        timeout: 30000,
      }),
    ),
  );
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
  };
  manager: {
    port: number;
    hostname: string;
    onData: (key: string, socket: net.Socket, data: IData) => Promise<void>;
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
  onDataWorker,
}: {
  key: string;
  onDataManager: (key: string, socket: net.Socket, data: IData) => Promise<void>;
  onDataWorker: (key: string, socket: net.Socket, data: IData) => Promise<void>;
  worker: {
    port: number;
    hostname: string;
    queueDir: string;
    queueLimit?: number;
  };
  manager: {
    port: number;
    hostname: string;
    queueDir: string;
    queueLimit?: number;
  }
}) {
  return combineLatest([
    runServer$({ key, onData: onDataManager, ...manager }),
    runServer$({ key, onData: onDataWorker, ...worker }),
  ]);
}

function runConnections$({
  onDataConnectionManager,
  onDataConnectionWorker,
  connections,
}: {
  onDataConnectionManager: (key: string, socket: net.Socket, data: IData) => Promise<void>;
  onDataConnectionWorker: (key: string, socket: net.Socket, data: IData) => Promise<void>;
  connections: {
    key: string;
    manager: {
      port: number;
      hostname: string;
    };
    worker: {
      port: number;
      hostname: string;
    };
  }[];
}) {
  return combineLatest(
    connections.map(config => runPairConnection$({
      key: config.key,
      manager: {
        ...config.manager,
        onData: onDataConnectionManager,
      },
      worker: {
        ...config.worker,
        onData: onDataConnectionWorker,
      },
    }))
  );
}

// function runServerData$({
//   port,
//   hostname,
//   queueDir,
// }: {
//   port: number;
//   hostname: string;
//   queueDir: string;
// }) {
//   return new Observable<{
//     socket: net.Socket;
//     data: IData;
//   }>(o => {
//     const server$ = runServer$({
//       hostname,
//       port,
//       queueDir,
//       onData: async (socket, data) => {
//         o.next({
//           socket, data,
//         })
//       },
//     }).subscribe({
//       error(error) {
//         o.error(error);
//       },
//       complete() {
//         o.complete();
//       },
//     });
//     o.add(() => server$.unsubscribe());
//   });
// }

// function convertCallbackData$<T>(
//   observable: (callback: (onCallbackData: () => Promise<T>) => Promise<void>) => Observable<any>
// ) {
//   return new Observable<T>(o => {
//     const observable$ = observable(
//       async (onCallbackData) => {
//         o.next(await onCallbackData());
//       }
//     ).subscribe({
//       error(error) {
//         o.error(error);
//       },
//       complete() {
//         o.complete();
//       },
//     });
//     o.add(() => observable$.unsubscribe());
//   });
// }

// function runWorkerData$({
//   port,
//   hostname,
//   stages,
// }: {
//   port: number;
//   hostname: string;
//   stages: IStages;
// }) {
//   return convertCallbackData$<{
//     socket: net.Socket;
//     data: IData;
//   }>(cb => runServer$({
//     hostname,
//     port,
//     queueDir: path.resolve("./data_queue"),
//     onData: async (socket, data) => await cb(
//       async () => {
//         return {
//           socket,
//           data,
//         }
//       }
//     )
//   })).pipe(
//     tap(({ socket, data }) => {
//       console.log(data);
//     })
//   );
// }

interface IProcessingStorage {
  onData: (data: { stage: string, key?: string, data: IData; eof: boolean }) => Promise<void>;
  onFinish: () => Promise<void>
}

interface IProcessing {
  totalSum?: number;
  processed: number;
  processes: number;
  storage: Map<string, IProcessingStorage>;
  usedGroups: string[];
  usedGroupsTotals: Map<string, number>,
}

type IProcessingState = Map<string, IProcessing>;

function getDefaultProcessingMap(): IProcessing {
  return {
    totalSum: null,
    processed: 0,
    processes: 0,
    storage: new Map(),
    usedGroups: [],
    usedGroupsTotals: new Map(),
  }
}

enum QuestionTypeEnum {
  GET_SESSION_STAGE_KEY_SERVER = "getSessionStageKeyServer",
  COUNT_PROCESSED = "countProcessed",
  CAN_GET_STAGE = "canGetStage",
}

enum NotificationTypeEnum {
  END_PROCESSING = "endProcessing",
  NULL_ACHIEVED = "nullAchieved",
}

interface IMachineState {
  ask?: (question: QuestionTypeEnum, data: IData) => Promise<IData>;
  notify?: (type: NotificationTypeEnum, data: IData) => Promise<void>;
  onData?: (data: ISendData) => Promise<void>;
}

interface IConfiguration {
  key: string;
  manager: {
    hostname: string;
    port: number;
    queueDir: string;
  },
  worker: {
    hostname: string;
    port: number;
    queueDir: string;
  },
  connections: {
    key: string;
    manager: {
      hostname: string;
      port: number;
    };
    worker: {
      hostname: string;
      port: number;
    };
  }[],
  stages: IStages;
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

interface IServerState {
  processingState: IProcessingState;
  machineState: IMachineState;
  sessionStageKeyCache: Map<string, ISessionStageKeyCache | ISessionStageKeyCachePromise>;
  connectionCache: Map<string, IConnectionCache | IConnectionCachePromise>;
  answers: Map<string, {
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

function runMachine$(configuration: IConfiguration) {
  return of<IServerState>({
    processingState: new Map(),
    sessionStageKeyCache: new Map(),
    connectionCache: new Map(),
    machineState: {},
    answers: new Map(),
  }).pipe(
    mergeMap(state => combineLatest([
      of(state),
      runPairServer$({
        key: configuration.key,
        manager: configuration.manager,
        worker: configuration.worker,
        onDataManager: async (key, socket, raw: IDataBase) => {
          const { id, question, type, data } = raw;
          if (id) {
            const answer = await state.machineState.ask(question as QuestionTypeEnum, data);
            socket.push(JSON.stringify({
              id,
              answer,
            }) + '\n');
          } else {
            await state.machineState.notify(type as NotificationTypeEnum, data);
          }
        },
        onDataWorker: async (key, socket, data) => {
          await state.machineState.onData(data as ISendData);
        },
      }),
      runConnections$({
        connections: configuration.connections,
        onDataConnectionManager: async (key, socket, raw: IDataBase) => {
          const { id, answer } = raw;
          state.answers.get(id as string).resolve(answer);
        },
        onDataConnectionWorker: async (key, socket, data) => { }, // Not used
      }),
    ])),
    map(([state, server, connections]) => {
      async function askAll(question: QuestionTypeEnum, data: IData) {
        const results: [string, IData][] = await Promise.all(connections.map(
          async ([key, manager, worker]): Promise<[string, IData]> => {
            const id = getId();
            const [promise, resolve] = getPromise();
            state.answers.set(id, {
              promise,
              resolve,
            })
            manager.push(JSON.stringify({
              id,
              question,
              data,
            }) + '\n');
            const answer: IData = await promise;
            state.answers.delete(id);
            return [key, answer];
          }
        ));
        return results.filter(([key, result]) => !!result);
      }

      async function ask(question: QuestionTypeEnum, data: IData) {
        let resolved = false;
        let result = await new Promise<[string, IData]>((r, e) => connections.forEach(
          async ([key, manager, worker]) => {
            const id = getId();
            const [promise, resolve] = getPromise();
            state.answers.set(id, {
              promise,
              resolve,
            });
            manager.push(JSON.stringify({
              id,
              question,
              data,
            }) + '\n');
            const answer = await promise;
            if (answer && !resolved) {
              resolved = true;
              r([key, answer]);
            }
            state.answers.delete(id);
          }
        ));
        return result;
      }

      async function notify(type: NotificationTypeEnum, data: IData) {
        connections.forEach(
          async ([key, manager, worker]) => {
            manager.push(JSON.stringify({
              type,
              data,
            }) + '\n');
          }
        );
      }

      state.machineState.ask = async (question, data) => {
        const { session, stage, group, key } = data as {
          session: string;
          group: string;
          stage: string;
          key: string;
        };
        switch (question) {
          case QuestionTypeEnum.CAN_GET_STAGE:
            return Object.keys(configuration.stages).includes(stage);
          case QuestionTypeEnum.COUNT_PROCESSED:
            const map = state.processingState.get(group);
            return map ? map.processed : 0;
          case QuestionTypeEnum.GET_SESSION_STAGE_KEY_SERVER:
            const hash = getHash(session, stage, key);
            return state.sessionStageKeyCache.has(hash)
              && (state.sessionStageKeyCache.get(hash) as ISessionStageKeyCache).connectionKey;
        }
        return null;
      };

      state.machineState.notify = async (type, data) => {
        const { group, totalSum } = data as {
          group: string;
          totalSum: number;
        };
        const map = group && state.processingState.get(group);
        switch (type) {
          case NotificationTypeEnum.NULL_ACHIEVED:
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
              map
                .storage
                .forEach((storage) => {
                  storage.onFinish();
                });

              map.usedGroups
                .forEach((nextGroup) => {
                  const totalSum = state.processingState.get(group).usedGroupsTotals.get(name);
                  notify(NotificationTypeEnum.NULL_ACHIEVED, {
                    group: nextGroup,
                    totalSum,
                  });
                });

              state.processingState.delete(group);
            }
            return;
        }
      };

      async function askSessionStageKeyServer(session: string, stage: string, key?: string) {
        const hash = getHash(session, stage, key);
        let sessionKeyCache = state.sessionStageKeyCache.get(hash) as ISessionStageKeyCache;
        let sessionKeyCachePromise = state.sessionStageKeyCache.get(hash) as ISessionStageKeyCachePromise;
        if (sessionKeyCachePromise && sessionKeyCachePromise.promise) {
          await sessionKeyCachePromise.promise;
          sessionKeyCache = state.sessionStageKeyCache.get(hash) as ISessionStageKeyCache;
        }
        if (!sessionKeyCache) {
          const [promise, resolve] = getPromise();
          state.sessionStageKeyCache.set(hash, { promise, resolve });
          const answer = await ask(
            QuestionTypeEnum.GET_SESSION_STAGE_KEY_SERVER,
            {
              session,
              stage,
              key,
            },
          );
          sessionKeyCache.connectionKey = answer && answer[1] as string;
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
          const answers = await askAll(QuestionTypeEnum.CAN_GET_STAGE, {
            stage,
          });
          connections = answers.map(([key, result]) => key);
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

      async function getSessionStageKeyConnectionScript(session: string, stage: string, key?: string) {
        let connectionKey;

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

      async function sendToServer(connectionKey, data) {
        if (!connectionKey) {
          throw Error("NO_CONNECTIONS_ERROR");
        }
        const message = JSON.stringify(data) + '\n';
        const [, , worker] = connections.find(([key]) => key === connectionKey);
        return worker.push(message);
      }

      // async function send({session, group, stage, key, data}) {
      //   const connectionKey = await getSessionStageKeyConnectionScript(
      //     session,
      //     stage,
      //     key,
      //   );
      //   await sendToServer(
      //     connectionKey,
      //     session,
      //     group,
      //     stage,
      //     key,
      //     data,
      //   );
      //   return connectionKey;
      // }

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
        if (configuration.key
          && (
            !connectionKey || configuration.key === connectionKey + "DISABLED"
          )
        ) {
          await state.machineState.onData(raw);
        } else {
          await sendToServer(
            connectionKey,
            raw
          );
        }
      }

      function checkProcessingMap(group: string) {
        if (!state.processingState.get(group)) {
          state.processingState.set(group, getDefaultProcessingMap());
        }
      }

      function checkStorageMap(group: string, hash: string) {
        const processingMap = state.processingState.get(group);
        if (!processingMap.storage.get(hash)) {
          processingMap.storage.set(hash, {
            onData: async () => { },
            onFinish: async () => { },
          });
        }
      }

      function getMapperScript(stage: string) {
        let mapper = configuration.stages[stage];
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

      function setUsedGroup(group: string, nextGroup: string) {
        if (state.processingState.get(group).usedGroups.indexOf(nextGroup) === -1) {
          state.processingState.get(group).usedGroups.push(nextGroup);
        }
      }

      function increaseUsedGroupSend(group, nextGroup) {
        if (state.processingState.get(group).usedGroupsTotals[nextGroup] == null) {
          state.processingState.get(group).usedGroupsTotals[nextGroup] = 0;
        }
        state.processingState.get(group).usedGroupsTotals[nextGroup]++;
      }

      function getSendCatchUsedGroupWrap(group: string, session: string): ISendFn {
        return async (stage: string, key: string, data: IData) => {
          const nextGroup = getHash(group, stage);
          setUsedGroup(group, nextGroup);
          increaseUsedGroupSend(group, nextGroup);
          await send({session, group: nextGroup, stage, key, data});
        };
      }

      async function buildMap(group: string, session: string, key: string, mapper: IStage): Promise<IProcessingStorage> {
        const sendWrap = getSendCatchUsedGroupWrap(group, session);
        const mapResult = await mapper(key, sendWrap);
        const mapCouple = parseMapperResult(mapResult);
        return {
          onData: mapCouple[0],
          onFinish: mapCouple[1],
        };
      }

      async function getStorage(group: string, hash: string, session: string, stage: string, key?: string) {
        let storage = state.processingState.get(group).storage.get(hash);
        if (!storage) {
          const mapper = getMapperScript(stage);
          storage = await buildMap(group, session, key, mapper);
        }
        return storage;
      }

      state.machineState.onData = async ({ session, group, stage, key, data }: ISendData) => {
        const hash = getHash(session, stage, key || getId());
        checkProcessingMap(group);
        state.processingState.get(group).processed++;
        state.processingState.get(group).processes++;
        checkStorageMap(group, hash);
        const storage = await getStorage(group, hash, session, stage, key);
        await storage.onData({ stage, key, data, eof: !data });
        if (!key) {
          state.processingState.get(group).storage.delete(hash);
        }
        const totalSum = state.processingState.get(group).totalSum;
        state.processingState.get(group).processes--;
        if (state.processingState.get(group).processes === 0 && totalSum != null) {
          const processedArray = await askAll(
            QuestionTypeEnum.COUNT_PROCESSED,
            {
              group,
            },
          );

          const processed = processedArray
            .reduce((value, [key, count]: [string, number]) => value + count, 0);

          if (processed === totalSum) {
            notify(NotificationTypeEnum.END_PROCESSING, {
              group,
            });
          }
        }
      };

      return [
        server,
        connections,
      ];
    }),
  );
}

runMachine$({
  key: 'local',
  worker: {
    hostname: '0.0.0.0',
    port: 3000,
    queueDir: path.resolve("./data_queue"),
  },
  manager: {
    hostname: '0.0.0.0',
    port: 3001,
    queueDir: path.resolve("./manager_queue"),
  },
  connections: [
    {
      key: 'local',
      worker: {
        hostname: 'localhost',
        port: 3000,
      },
      manager: {
        hostname: 'localhost',
        port: 3001,
      },
    }
  ],
  stages: {
    init: async (key, send) => {
      let state = false;
      return [
        async (data) => {
          state = !state;
          await send("reduce_2_flows", state ? "final" : "final_alt", data);
        },
        () => {
          // console.log('Finished! init');
        },
      ];
    },
    reduce_2_flows: async (key, send) => {
      return [
        async (data) => {
          await send("map", null, data);
        },
        () => {
          console.log('Finished 2 flows!');
        },
      ];
    },
    map: async (key, send) => {
      return [
        async (data) => {
          await send("final_reduce", "final", data);
          // console.log("map", kkk++);
        },
        () => {
          // console.log('Finished map!');
        },
      ];
    },
    final_reduce: async (key) => {
      let sum = 0;
      const timeStarted = +new Date();
      return [
        async (data) => {
          sum++;
          console.log("final_reduce", sum);
        },
        () => {
          const timePassed = ((+new Date()) - timeStarted) / 1000;
          const minutes = Math.floor(timePassed / 60);
          const seconds = Math.floor(timePassed % 60);
          console.log('Finished!', sum, `${minutes} min`, `${seconds} sec`);
        },
      ];
    }
  }
}).subscribe(state => {
  console.log(state);
});