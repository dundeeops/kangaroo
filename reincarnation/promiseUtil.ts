export function getPromise(): [
    Promise<any>, Function, Function
] {
    let resolve = () => {};
    let reject = () => {};
    const promise = new Promise((r, e) => {
        resolve = r;
        reject = e;
    });
    return [promise, resolve, reject];
}

export async function raceData<T>(promises: Promise<T>[]) {
    let resolved = false;
    return await new Promise((r, e) => {
        promises.forEach((promise) => {
            promise.then((data) => {
                if (data) {
                    resolved = true;
                    r(data);
                }
            }).catch((error) => e(error));
        });

        Promise.all(promises).then(() => {
            if (resolved === false) {
                r();
            }
        })
    })
}
