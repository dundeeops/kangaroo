const TimeoutErrorTimer = require("../engine/TimeoutErrorTimer.js");
const FakeTimeout = require("./FakeTimeout.js");

describe("TimeoutErrorTimer", () => {

    test("should start", () => {
        const fakeTimeout = new FakeTimeout();

        errorTimer = new TimeoutErrorTimer({
            onError: jest.fn(),
            message: "TEST",
            timeout: 125,
            inject: {
                _setTimeout: fakeTimeout.getSetTimeout(),
                _clearTimeout: fakeTimeout.getClearTimeout(),
            },
        });

        errorTimer.start();

        const {
            isStared, isStopped, timeout,
        } = fakeTimeout.getState();

        expect(timeout).toBe(125);
        expect(isStared).toBe(true);
        expect(isStopped).toBe(false);
    });

    test("should start & stop", () => {
        const fakeTimeout = new FakeTimeout();

        errorTimer = new TimeoutErrorTimer({
            onError: jest.fn(),
            message: "TEST",
            timeout: 125,
            inject: {
                _setTimeout: fakeTimeout.getSetTimeout(),
                _clearTimeout: fakeTimeout.getClearTimeout(),
            },
        });

        errorTimer.start();
        errorTimer.stop();

        const {
            isStared, isStopped, timeout,
        } = fakeTimeout.getState();

        expect(timeout).toBe(125);
        expect(isStared).toBe(true);
        expect(isStopped).toBe(true);
    });

    test("should catch error", () => {
        const fakeTimeout = new FakeTimeout();
        let error = null;

        errorTimer = new TimeoutErrorTimer({
            onError: (_error) => error = _error,
            message: "TEST",
            timeout: 125,
            inject: {
                _setTimeout: fakeTimeout.getSetTimeout(),
                _clearTimeout: fakeTimeout.getClearTimeout(),
            },
        });

        errorTimer.start();
        fakeTimeout.simulateOnTick();

        const {
            isStared, isStopped, timeout,
        } = fakeTimeout.getState();

        expect(timeout).toBe(125);
        expect(isStared).toBe(true);
        expect(isStopped).toBe(false);
        expect(error.message).toBe("TEST");
    });
});
