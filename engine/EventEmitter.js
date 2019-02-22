const EventEmitterPrototype = require("events");

// TODO: Remove EventEmitter
module.exports = class EventEmitter extends EventEmitterPrototype {
    constructor() {
        super();
    }

    async onceIfTrue(name, callback) {
        let callbackWrapper;
        callbackWrapper = function(...args) {
            const result = callback(...args);
            if (!result) {
                this.prependOnceListener(name, callbackWrapper.bind(this));
            }
        }
        this.prependOnceListener(name, callbackWrapper.bind(this));
    }

    async raceData(promises) {
        let resolved = false;
        return await new Promise((r, e) => {
            promises.forEach((promise) => {
                promise.then((data) => {
                    if (data) {
                        resolved = true;
                        r(data);
                    }
                }).catch((error) => e(error));
            });

            Promise.all(promises).then(() => {
                if (resolved === false) {
                    r();
                }
            })
        })
    }
}
