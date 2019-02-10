const net = require('net');

exports.serverFactory = (options) => {
    const server = net.createServer(options.prepare);
    server.listen(options.port, options.hostname);
    return server;
}