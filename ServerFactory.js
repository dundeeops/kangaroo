const net = require('net');

exports.ServerFactory = (options) => {
    const server = net.createServer(options.prepare);
    server.listen(options.port, options.hostname, () => options.onConnect());
    return server;
}