import { Readable } from "stream";
import path from "path";
import { runMachine$ } from "./src/index";

const timeStarted = +new Date();

runMachine$({
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
  server: {
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
    stages: {
      init: async (key, send) => {
        let state = Math.random() > 0.5;
        return async ({ data }) => {
          state = !state;
          await send("reduce_2_flows", state ? "final" : "final_alt", data);
        };
      },
      reduce_2_flows: async (key, send) => {
        return [
          async ({ data }) => {
            await send("map", null, data);
          },
          () => {
            console.log('Finished 2 flows!');
          },
        ];
      },
      map: async (key, send) => {
        return async ({ data }) => {
          await send("final_reduce", "final", data);
        };
      },
      final_reduce: async (key) => {
        let sum = 0;
        return [
          async ({ data }) => {
            sum++;
            // console.log("final_reduce", sum);
          },
          () => {
            const timePassed = ((+new Date()) - timeStarted) / 1000;
            const milliseconds = ((+new Date()) - timeStarted) % 1000;
            const minutes = Math.floor(timePassed / 60);
            const seconds = Math.floor(timePassed % 60);
            console.log('Finished!', sum, `${minutes} min`, `${seconds} sec`, `${milliseconds} ms`);
          },
        ];
      }
    }
  }
}).subscribe(state => {
  console.log(state);

  const max = 100000;
  let index = 0;
  state.runStream(
    "init",
    null,
    new Readable({
      read() {
        if (max <= index) {
          this.push(null);
          console.log('The stream has been send! Processing on the cluster...');
        } else {
          this.push(Buffer.from(String(index) + "\n", 'utf8'));
          index++;
        }
      },
    }),
  );
});
