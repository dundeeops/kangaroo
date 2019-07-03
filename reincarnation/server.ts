import { of, from, timer, Observable, throwError, combineLatest } from "rxjs";
import { map, tap, switchMap, retryWhen, delayWhen, mergeMap, share, concat, concatMap } from "rxjs/operators";
import net from "net";
import domain from "domain";
import path from "path";
import { SplitTransformStream } from "./splitTransformStream";
import { getHash } from "./serializationUtil";
import { queueDuplexStream } from "./queueDuplexStream";

interface IDataBase {
  [key: string]: string | number | boolean | IDataBase | Array<string | number | boolean | IDataBase>;
}

type IData = string | number | boolean | IDataBase | Array<string | number | boolean | IDataBase>;

type IStage = (key: string, send: (stage: string, key: string, data: IData) => Promise<void>) => Promise<void>

interface IStages {
  [key: string]: IStage;
}

function makeServer({
  port,
  hostname,
  onSocket,
  onConnect,
  onError,
  onClose
}: {
  port: number;
  hostname: string;
  onSocket: (socket: net.Socket) => void;
  onConnect: Function;
  onError: Function;
  onClose: Function;
}): net.Server {
  const server = net.createServer(onSocket);
  server.on("close", () => onClose());
  server.on("error", (error) => onError(error));
  server.on("listening", () => onConnect());
  server.listen(port, hostname);
  return server;
}

async function connectServer({
  port,
  hostname,
  onSocket,
}: {
  port: number;
  hostname: string;
  onSocket: (socket: net.Socket) => void;
}): Promise<net.Server> {
  return new Promise((r, e) => makeServer({
    port,
    hostname,
    onSocket,
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

export const retryStrategy = <T>({
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
      `Attempt ${retryAttempt}: retrying in ${retryAttempt *
        scalingDuration}ms`
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
        retryStrategy<T>({
          maxRetryAttempts: 3,
          scalingDuration: 1000,
        }),
      ),
    )),
  );
}

function runDomainServer$({
  port,
  hostname,
  onSocket,
}: {
  port: number;
  hostname: string;
  onSocket: (socket: net.Socket) => void;
}) {
  return runDomain$(async () => await connectServer({
    hostname,
    port,
    onSocket,
  }));
}

function runServer$({
  port,
  hostname,
  queueDir,
  onData,
}: {
  port: number;
  hostname: string;
  queueDir: string;
  onData: (socket: net.Socket, data: IData) => Promise<void>;
}) {
  return runDomainServer$({
    hostname,
    port,
    onSocket: (socket) => {
      socket
        .pipe(new SplitTransformStream())
        .pipe(queueDuplexStream({
          readableStream: socket,
          concurrency: 10,
          dir: queueDir,
          fn: async (line) => {
            await onData(socket, JSON.parse(line));
          },
          memoryLimit: 1000,
        }))
    },
  });
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

function convertCallbackData$<T>(
  observable: (callback: (onCallbackData: () => Promise<T>) => Promise<void>) => Observable<any>
) {
  return new Observable<T>(o => {
    const observable$ = observable(
      async (onCallbackData) => {
        o.next(await onCallbackData());
      }
    ).subscribe({
      error(error) {
        o.error(error);
      },
      complete() {
        o.complete();
      },
    });
    o.add(() => observable$.unsubscribe());
  });
}

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

interface IProcessingStorageMap {
  onData: (data: { stage: string, key?: string, data: IData; eof: boolean }) => Promise<void>;
  onFinish: () => Promise<void>
}

interface IProcessingStorage {
  totalSum?: number;
  processed: number;
  processes: number;
  storageMap: Map<string, IProcessingStorageMap>;
  usedGroups: string[];
  usedGroupsTotals: {
    [key: string]: number;
  },
}

interface IProcessing {
  totalSum?: number;
  processed: number;
  processes: number;
  storageMap: Map<string, IProcessingStorage>;
  usedGroups: string[];
  usedGroupsTotals: {},
}

function getDefaultProcessingMap(): IProcessing {
  return {
    totalSum: null,
    processed: 0,
    processes: 0,
    storageMap: new Map(),
    usedGroups: [],
    usedGroupsTotals: {},
  }
}

async function send(session, group, stage, key, data) {
  const connectionKey = await getSessionStageKeyConnectionScript(
      session,
      stage,
      key,
  );
  if (!connectionKey || _key === connectionKey + "DISABLED") {
      await onData(null, {
          session,
          group,
          stage,
          key,
          data,
      });
  } else {
      await sendToServer(
          connectionKey,
          session,
          group,
          stage,
          key,
          data,
      );
  }
  return connectionKey;
}

function getSendCatchUsedGroupWrap(group, session) {
  return async (stage, key, data) => {
    const nextGroup = getHash(group, stage);
    setUsedGroup(group, nextGroup);
    increaseUsedGroupSend(group, nextGroup);
    await send(session, nextGroup, stage, key, data);
  };
}

async function getProcessingStorageMap(group, session, key, mapper): Promise<IProcessingStorageMap> {
  const sendWrap = getSendCatchUsedGroupWrap(group, session);
  const mapResult = await mapper(key, sendWrap);
  const mapCouple = parseMapperResult(mapResult);
  return {
    onData: mapCouple[0],
    onFinish: mapCouple[1],
  };
}

function runWorker$({
  port,
  hostname,
  stages,
}: {
  port: number;
  hostname: string;
  stages: IStages;
}) {
  return of<Map<string, IProcessing>>(new Map()).pipe(
    concatMap(maps => combineLatest([
      of(maps),
      of(async (socket: net.Socket, data: IData) => {

      }),
    ])),
    concatMap(([maps, onData]) => combineLatest([
      of(maps),
      runServer$({
        hostname,
        port,
        queueDir: path.resolve("./data_queue"),
        onData,
      })
    ])),
    tap(a => console.log(a))
  );
}

of({
  key: 'local',
  worker: {
    hostname: '0.0.0.0',
    port: 3000,
  },
  manager: {
    hostname: '0.0.0.0',
    port: 3001,
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
  mapping: {
    init: (key, send) => {
      let state = false;
      return [
        async ({ data }) => {
          state = !state;
          await send("reduce_2_flows", state ? "final" : "final_alt", data);
        },
        () => {
          // console.log('Finished! init');
        },
      ];
    },
    reduce_2_flows: (key, send) => {
      return [
        async ({ data }) => {
          await send("map", null, data);
        },
        () => {
          console.log('Finished 2 flows!');
        },
      ];
    },
    map: (key, send) => {
      return [
        async ({ data }) => {
          await send("final_reduce", "final", data);
          // console.log("map", kkk++);
        },
        () => {
          // console.log('Finished map!');
        },
      ];
    },
    final_reduce: (key) => {
      let sum = 0;
      const timeStarted = +new Date();
      return [
        async ({ data }) => {
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
}).pipe(
  map(conf => runWorker$(conf))
).toPromise();