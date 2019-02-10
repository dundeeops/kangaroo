const fs = require("fs");
const path = require("path");
const yaml = require("js-yaml");

const readConfig = () => yaml.safeLoad(
    fs.readFileSync(
        path.resolve("./config.yml"),
        "utf8"
    )
);

module.exports = config = reaadConfig();