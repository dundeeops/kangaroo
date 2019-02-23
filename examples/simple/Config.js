const fs = require("fs");
const path = require("path");
const yaml = require("js-yaml");
const argv = require('yargs').argv;

const defaultConfig = {};
const defauilPath = path.resolve((argv.config || "./config.yml"));

function readYamlFile(pathToFile, _parseYaml = yaml.safeLoad, _readFile = fs.readFileSync) {
    return _parseYaml(
        _readFile(
            pathToFile,
            "utf8"
        ),
    );
}

const readConfig = (_readYamlFile = readYamlFile) => {
    const yamlConfig = _readYamlFile(defauilPath);

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
