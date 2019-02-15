module.exports = {
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
}