module.exports = {

    async startUnlessTimeout(callback, timeout, _setTimeout = setTimeout) {
        const func = async () => {
            const result = await callback();
            if (result) {
                _setTimeout(func, timeout);
            }
        }
        await func();
    },

    getPromise() {
        let resolve = () => {};
        let reject = () => {};
        const promise = new Promise((r, e) => {
            resolve = r;
            reject = e;
        });
        return [promise, resolve, reject];
    },

    promisify(obj, keys) {
        keys.forEach((key) => {
            const fn = obj[key];
            function wrap(...args) {
                const instance = this;
                
                return new Promise((r, e) => {
                    fn.apply(instance, [...args, (err, res) => {
                        if (err) {
                            e(err);
                        } else {
                            r(res);
                        }
                    }]);
                })
            }
            obj[key + "Async"] = wrap;
        });
    },

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