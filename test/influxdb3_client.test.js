const mockWrite = jest.fn();

jest.mock('@influxdata/influxdb3-client', () => ({
    InfluxDBClient: jest.fn().mockImplementation(() => ({
        write: mockWrite,
        close: jest.fn()
    }))
}));

describe('writeWithRetry', () => {
    let writeWithRetry;
    beforeEach(() => {
        jest.resetModules();
        jest.useFakeTimers();
        mockWrite.mockReset();
        ({ writeWithRetry } = require('../migrate_v2_to_v3/influxdb3_client'));
    });

    afterEach(() => {
        jest.useRealTimers();
    });

    test('resolves when write succeeds', async () => {
        const result = 'ok';
        mockWrite.mockResolvedValue(result);
        await expect(writeWithRetry(['line'])).resolves.toBe(result);
        expect(mockWrite).toHaveBeenCalledTimes(1);
    });

    test('retries on failure and eventually resolves', async () => {
        mockWrite
            .mockRejectedValueOnce(new Error('fail'))
            .mockResolvedValueOnce('ok');

        const promise = writeWithRetry(['line']);

        // first attempt
        await Promise.resolve();
        expect(mockWrite).toHaveBeenCalledTimes(1);

        // advance time for backoff (300ms)
        await jest.advanceTimersByTimeAsync(300);
        await Promise.resolve();

        expect(mockWrite).toHaveBeenCalledTimes(2);
        await expect(promise).resolves.toBe('ok');
    });
});