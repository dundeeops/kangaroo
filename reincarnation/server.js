async function onServerError() {

}

async function onServerData(data, socket) {

}

async function onServerSocket(socket) {

}

function server(options = {

}) {


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
      return [
        async ({ data }) => {
          sum++;
          console.log("final_reduce", sum);
        },
        () => {
          const timePassed = (new Date() - timeStarted) / 1000;
          const minutes = Math.floor(timePassed / 60);
          const seconds = Math.floor(timePassed % 60);
          console.log('Finished!', sum, `${minutes} min`, `${seconds} sec`);
        },
      ];
    }
  }
})
  .pipe(
    map(conf => server(conf))
  ).toPromise();