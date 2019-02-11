const net = require('net');

module.exports = class ServerConnection {
    constructor(options) {
        this.name = options.name;
        this.hostname = options.hostname;
        this.port = options.port;
    }

    connect() {
        this.socket = new net.Socket();
        this.socket.connect(
            this.port,
            this.hostname,
            () => {
                console.log('Connected');
            },
        );

        this.socket.on('data', (data) => {
            console.log('Received: ' + data);
        });

        this.socket.on('close', () => {
            console.log('Connection closed');
            this.socket = null;
        });
    }

    destroy() {
        if (this.socket) {
            this.socket.destroy();
        }
    }

    sendData(data) {
        this.socket.write(data);
    }
}
