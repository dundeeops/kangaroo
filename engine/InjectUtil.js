module.exports = {
    inject(name, defaultObject, obj) {
        if (obj && typeof obj === "string" && obj[name]) {
            return obj[name];
        } else {
            return defaultObject;
        }
    },
}