const path = require("path");
const fs = require("fs");
const {
    parseData,
    serializeData,
} = require("./SerializationUtil");

async function deleteFolderRecursive(parentPath) {
    if (fs.existsSync(parentPath)) {
        fs.readdirSync(parentPath).forEach((file) => {
            const curPath = path.resolve(parentPath, file);
            if (fs.lstatSync(curPath).isDirectory()) {
                deleteFolderRecursive(curPath);
            } else {
                fs.unlinkSync(curPath);
            }
        });
        fs.rmdirSync(parentPath);
    }
}

async function makeDirIfNotExist(dir) {
    if (!fs.existsSync(dir)){
        fs.mkdirSync(dir);
    }
}

async function readFileJson(filePath) {
    return parseData(
        fs.readFileSync(filePath),
    );
}

async function writeFileJson(filePath, data) {
    fs.writeFileSync(
        filePath,
        serializeData(data),
    );
}

module.exports = {
    deleteFolderRecursive,
    makeDirIfNotExist,
    readFileJson,
    writeFileJson,
}