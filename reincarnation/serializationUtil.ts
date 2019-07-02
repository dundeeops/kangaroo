import safeStringify from "fast-safe-stringify";
import cr from "crypto";

export const ENDING = "\n";
const SEPARATOR = ":_:";

export function cleanString(str: string) {
    return str.toString().replace(/(\n|\r)$/, "").trim();
}

export function serializeData(data): string {
    return safeStringify(data);
}

export function makeMessage(data): string {
    return serializeData(data) + ENDING;
}

export function parseData(raw) {
    return JSON.parse(cleanString(raw));
}

export function getHash(...args) {
    return cr
        .createHash("sha1")
        .update(
            args.join(SEPARATOR),
        )
        .digest("base64");
}

export function getId() {
    return cr.randomBytes(64).toString("hex");
}
