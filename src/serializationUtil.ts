import cr from 'crypto';

const SEPARATOR = ':_:';

export function getHash(...args: string[]) {
    return cr
        .createHash('sha1')
        .update(
            args.join(SEPARATOR),
        )
        .digest('base64');
}

export function getId() {
    return cr.randomBytes(64).toString('hex');
}
