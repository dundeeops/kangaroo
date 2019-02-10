const fs = require("fs");
const { Transform } = require('stream');
const { StringDecoder } = require('string_decoder');

const file = "data.txt";

class StringTransform extends Transform {
    constructor(options) {
      super(options);
      this._decoder = new StringDecoder(options && options.defaultEncoding);
      this.data = '';
    }

    _transform(chunk, encoding, callback) {
        let data;

        if (encoding === 'buffer') {
            data = this._decoder.write(chunk);
        } else {
            data = chunk;
        }

        // this.data += data;
        console.log(`%${data}%`);

        // this.push(data + "hkjh");
        callback(null, data + "hkjh");
    //   this[kSource].fetchSomeData(size, (data, encoding) => {
    //     this.push(Buffer.from(data, encoding));
    //   });
    }

    _final(callback) {
        let data = this._decoder.end();
        this.push(data);
        callback();
    }

    // _read(size) {
    // //   this[kSource].fetchSomeData(size, (data, encoding) => {
    // //     this.push(Buffer.from(data, encoding));
    // //   });
    // }

    // _write(chunk, encoding, callback) {
    //   if (encoding === 'buffer') {
    //     chunk = this._decoder.write(chunk);
    //   }
    //   this.data += chunk;
    //   console.log(`%${chunk}%`)
    //   callback();
    // }

    // _final(callback) {
    //   this.data += this._decoder.end();
    //   callback();
    // }
  }
  

const myTransform = new StringTransform();
const myTransform2 = new StringTransform();

const readFile = fs.createReadStream(file, { encoding: "utf8" })

readFile.pipe(myTransform);
myTransform.pipe(myTransform2);

myTransform2.on("data", data => {
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
