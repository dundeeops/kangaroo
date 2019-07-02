import { of, from, timer, Observable, throwError } from "rxjs";
import { map, tap, switchMap, retryWhen, delayWhen, mergeMap, share } from "rxjs/operators";
import net from "net";
import domain from "domain";
import path from "path";
import { SplitTransformStream } from "./splitTransformStream";
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

function runServerData$({
  port,
  hostname,
  queueDir,
}: {
  port: number;
  hostname: string;
  queueDir: string;
}) {
  return new Observable<{
    socket: net.Socket;
    data: IData;
  }>(o => {
    const server$ = runServer$({
      hostname,
      port,
      queueDir,
      onData: async (socket, data) => {
        o.next({
          socket, data,
        })
      },
    }).subscribe({
      error(error) {
        o.error(error);
      },
      complete() {
        o.complete();
      },
    });
    o.add(() => server$.unsubscribe());
  });
}

async function runWorker$({
  port,
  hostname,
  stages,
}: {
  port: number;
  hostname: string;
  stages: IStages;
}) {
  return runServerData$({
    hostname,
    port,
    queueDir: path.resolve("./data_queue"),
  }).pipe(
    tap(({ socket, data }) => {
      console.log(data);
    })
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