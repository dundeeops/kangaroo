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

type IStage = (key: string, send: (stage: string, key: string, data: IData) => Promise<void>) => Promise<IStageFunction>

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
  return new Promise((r, e) => makeServer({
    key,
    port,
    hostname,
    queueDir,
    queueLimit,
    onData,
    onConnect() {
      r();
    },
    onError(error) {
      e(error);
    },
    onClose() {
      e();
    },
  }));
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
  return new Promise((r, e) => makeConnection({
    key,
    port,
    hostname,
    onData,
    onConnect() {
      r();
    },
    onError(error) {
      e(error);
    },
    onClose() {
      e();
    },
  }));
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
  usedGroupsTotals: {},
}

type IProcessingState = Map<string, IProcessing>;

function getDefaultProcessingMap(): IProcessing {
  return {
    totalSum: null,
    processed: 0,
    processes: 0,
    storage: new Map(),
    usedGroups: [],
    usedGroupsTotals: {},
  }
}

// async function send(session, group, stage, key, data) {
//   const connectionKey = await getSessionStageKeyConnectionScript(
//       session,
//       stage,
//       key,
//   );
//   if (!connectionKey || _key === connectionKey + "DISABLED") {
//       await onData(null, {
//           session,
//           group,
//           stage,
//           key,
//           data,
//       });
//   } else {
//       await sendToServer(
//           connectionKey,
//           session,
//           group,
//           stage,
//           key,
//           data,
//       );
//   }
//   return connectionKey;
// }

// function getSendCatchUsedGroupWrap(group, session) {
//   return async (stage, key, data) => {
//     const nextGroup = getHash(group, stage);
//     setUsedGroup(group, nextGroup);
//     increaseUsedGroupSend(group, nextGroup);
//     await send(session, nextGroup, stage, key, data);
//   };
// }

// async function getProcessingStorage(group, session, key, mapper): Promise<IProcessingStorage> {
//   const sendWrap = getSendCatchUsedGroupWrap(group, session);
//   const mapResult = await mapper(key, sendWrap);
//   const mapCouple = parseMapperResult(mapResult);
//   return {
//     onData: mapCouple[0],
//     onFinish: mapCouple[1],
//   };
// }

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
  send?: (workerKey: string, data: {
    session: string;
    group: string;
    stage: string;
    key?: string;
    data: IData;
  }) => Promise<void>;
  onData?: (data: IData) => Promise<void>;
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

interface IServerState {
  processingState: IProcessingState;
  machineState: IMachineState;
  sessionStageKeyCache: Map<string, {
    connectionKey: string;
    promise: Promise<void>;
    resolve: Function;
  }>;
  connectionCache: Map<string, {
    date: number;
    connections: string[];
    promise: Promise<void>;
    resolve: Function;
  }>;
  answers: Map<string, {
    promise: Promise<void>;
    resolve: Function;
  }>;
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
        onDataManager: async (key, socket, data) => {

        },
        onDataWorker: async (key, socket, data) => {

        },
      }),
      runConnections$({
        connections: configuration.connections,
        onDataConnectionManager: async (key, socket, data) => {

        },
        onDataConnectionWorker: async (key, socket, data) => {}, // Not used
      }),
    ])),
    map(([state, server, connections]) => {
      const askAll = async (question, data) => {
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
            }));
            const result: IData = await promise;
            state.answers.delete(id);
            return [key, result];
          }
        ));
        return results.filter(([key, result]) => !!result);
      }

      const ask = async (question, data) => {
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
            }));
            const result = await promise;
            if (result && !resolved) {
              resolved = true;
              r([key, result]);
            }
            state.answers.delete(id);
          }
        ));
        return result;
      }

      const notify = async (type, data) => {
        connections.forEach(
          async ([key, manager, worker]) => {
            manager.push(JSON.stringify({
              type,
              data,
            }));
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
              && state.sessionStageKeyCache.get(hash).connectionKey;
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
                  const totalSum = this._processingMap.get(group).usedGroupsTotals[name];
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

      state.machineState.send = async (serverKey, data) => {
        const [, , worker] = connections.find(([key]) => key === serverKey);
        worker.push(JSON.stringify(data));
      };

      return true;
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
        hostname: '0.0.0.0',
        port: 3000,
      },
      manager: {
        hostname: '0.0.0.0',
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