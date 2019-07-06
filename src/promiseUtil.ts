export function getPromise(): [
    Promise<any>, Function, Function
] {
    let resolve;
    let reject;
    const promise = new Promise((r, e) => {
        resolve = r;
        reject = e;
    });
    return [promise, resolve, reject];
}
