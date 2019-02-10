const {serverFactory} = require("./server.js");

serverFactory({
    port: 1337,
    hostname: '0.0.0.0',
    prepare: (socket) => {
        socket.write('Echo server\r\n');
        socket.on('data', function(data){
            console.log(data);
            textChunk = data.toString('utf8');
            console.log(textChunk);
            // socket.write(textChunk);
        });
    }
});