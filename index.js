const fs = require("fs");
const { Transform, Writable } = require('stream');
const { StringDecoder } = require('string_decoder');

const net = require('net');

const file = "data.txt";

const pool = {
    specs: [
        {
            hostname: '127.0.0.1',
            port: 1337,
            lastHealthCheck: 0,
            socket: null,
        },
    ],
    init: function (index) {
        this.servers[index].socket = new net.Socket();
        this.servers[index].socket.connect(
            this.servers[index].port,
            this.servers[index].hostname,
            () => {
                console.log('Connected');
            },
        );

        this.servers[index].socket.on('data', (data) => {
            console.log('Received: ' + data);
            // client.destroy();
        });

        this.servers[index].socket.on('close', () => {
            console.log('Connection closed');
            this.servers[index].socket = null;
        });
    },
    sendData: function (index, data) {
        this.servers[index].socket.write(data);
    }
}

pool.init();
pool.sendData("TEST");

class StringToLinesTransform extends Transform {
    constructor(options) {
        super(options);
        this._decoder = new StringDecoder(options && options.defaultEncoding);
        this.cacheStr = '';
    }

    pushData(str) {
        const data = this.cacheStr + str;
        const chunks = data.split("\n");

        for (let i = 0; i < chunks.length - 1; i++) {
            this.push(chunks[i]);
        }

        if (chunks[chunks.length - 1]) {
            this.cacheStr = chunks[chunks.length - 1];
        } else {
            this.cacheStr = "";
        }
    }

    _transform(chunk, encoding, callback) {
        let data;

        if (encoding === 'buffer') {
            data = this._decoder.write(chunk);
        } else {
            data = chunk;
        }

        this.pushData(data);

        callback();
    }

    _final(callback) {
        let data = this._decoder.end();
        this.pushData(data);
        this.push(this.cacheStr);
        callback();
    }
}


class SendToProcessWritable extends Writable {
    constructor(options) {
        super(options);
        this._decoder = new StringDecoder(options && options.defaultEncoding);
        this._pool = options && options.pool;
    }

    _write(bytes, encoding, callback) {
        let chunk;

        if (encoding === 'buffer') {
            chunk = this._decoder.write(bytes);
        } else {
            chunk = bytes;
        }

        if (chunk) {
            this._pool.sendData(0, chunk);
        }

        callback();
    }

    _final(callback) {
        const chunk = this._decoder.end();

        if (chunk) {
            this._pool.sendData(0, chunk);
        }

        callback();
    }
}


const stringToLinesStream = new StringToLinesTransform();
const sendToProcessStream = new SendToProcessWritable({
    pool,
});

const readFile = fs.createReadStream(file, { encoding: "utf8" })

readFile.pipe(stringToLinesStream);
stringToLinesStream.pipe(sendToProcessStream);

stringToLinesStream.on("data", data => {
    console.log("data", data.toString());
})
.on("end", () => {
    console.log("end");
});

// const stream = fs.createReadStream(file, { encoding: "utf8" });
// stream.on("data", data => {
//     console.log(data.length);

//     // header = data.split(/\n/)[0];
//     // stream.destroy();
// });
// stream.on("close", () => {
//     //   console.timeEnd(label);
//     //   resolve();
// });
