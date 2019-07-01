const QueueService = require("../../engine/QueueService.js");

async function run() {
    const q = new QueueService();
    await q.initCache();

    let income = 0;
    let outcome = 0;

    // for (let index = 0; index < 100000; index++) {
    //     await q.push('test_' + index);
    //     income++;
    // }

    for (let index = 0; index < 100005; index++) {
        const data = await q.pop();
        outcome++;
        // console.log(data);
    }

    console.log(income, outcome);
}

run();
