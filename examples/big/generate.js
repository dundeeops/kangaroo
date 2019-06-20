const fs = require("fs");
const stream = require("stream");

const max = 1000000;
let index = 0;
new stream.Readable({
  read() {
    if (max <= index) {
      this.push(null);
    } else {
      this.push(Buffer.from(String(index) + "\n", 'utf8'));
      index++;
    }
  }
})
.pipe(fs.createWriteStream('big_data.txt'));