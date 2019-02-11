const fs = require("fs");
const path = require("path");
const yaml = require("js-yaml");
const argv = require('yargs').argv;

const readConfig = () => {
    const yamlConfig = yaml.safeLoad(
        fs.readFileSync(
            path.resolve(argv.config || "./config.yml"),
            "utf8"
        )
    );

    const name = argv.name || yamlConfig.name;

    return {
        ...yamlConfig,
        name,
    }
};

module.exports = config = readConfig();