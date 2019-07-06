import cr from "crypto";

export const ENDING = "\n";
const SEPARATOR = ":_:";

export function makeMessage(data): string {
    return JSON.stringify(data) + ENDING;
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
