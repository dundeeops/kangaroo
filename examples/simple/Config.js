const fs = require("fs");
const path = require("path");
const yaml = require("js-yaml");
const argv = require("yargs").argv;

const defaultConfig = {};
const defaultPath = path.resolve((argv.config || "./config.yml"));

const DEFAULT_ENCODING = "utf8";

function readYamlFile(pathToFile, _parseYaml = yaml.safeLoad, _readFile = fs.readFileSync) {
    return _parseYaml(
        _readFile(
            pathToFile,
            DEFAULT_ENCODING
        ),
    );
}

const readConfig = (_readYamlFile = readYamlFile) => {
    const yamlConfig = _readYamlFile(defaultPath);

    return {
        ...defaultConfig,
        ...yamlConfig,
    }
};

module.exports = {
    config: readConfig(),
    readConfig,
    readYamlFile,
};
