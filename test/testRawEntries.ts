import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { readRawEntries } from '../src/lib/rawEntries';
import type { RawScanOptions } from '../src/lib/rawEntries';
import { getFilenameForID, ts2day } from '../src/lib/getHistory';
import type { IobDataEntry } from '../src/lib/types';

const ID = 'javascript.0.raw';

/** Local noon of a day in January 2024 plus some ms, so that the day of a timestamp never depends on the time zone */
function tsOf(day: number, ms = 0): number {
    return new Date(2024, 0, day, 12, 0, 0).getTime() + ms;
}

function entry(ts: number, val: number): IobDataEntry {
    return { ts, val, ack: 1, q: 0 } as IobDataEntry;
}

describe('Test raw entries', function () {
    let storeDir: string;
    let warnings: string[];

    /** Store the entries in the files of their days, as `appendFile` does */
    function writeDays(entries: IobDataEntry[], id = ID): void {
        const byDay = new Map<string, IobDataEntry[]>();
        for (const e of entries) {
            const day = ts2day(e.ts);
            byDay.set(day, [...(byDay.get(day) || []), e]);
        }
        for (const [day, list] of byDay) {
            fs.mkdirSync(`${storeDir}${day}`, { recursive: true });
            fs.writeFileSync(getFilenameForID(storeDir, day, id), JSON.stringify(list, null, 2));
        }
    }

    function read(options: Partial<RawScanOptions>): ReturnType<typeof readRawEntries> {
        return readRawEntries({
            storeDir,
            id: ID,
            sort: 'asc',
            maxEntries: 100000,
            log: { warn: (message: string) => warnings.push(message) },
            ...options,
        });
    }

    const tsList = (entries: IobDataEntry[]): number[] => entries.map(e => e.ts);

    beforeEach(function () {
        storeDir = `${fs.mkdtempSync(path.join(os.tmpdir(), 'history-raw-'))}/`;
        warnings = [];
    });

    afterEach(function () {
        fs.rmSync(storeDir, { recursive: true, force: true });
    });

    it('returns the entries of the range with start and end inclusive, in both orders', function () {
        // three entries on each of the days 1 - 4
        const all = [1, 2, 3, 4].flatMap(day => [0, 1, 2].map(i => entry(tsOf(day, i), day * 10 + i)));
        writeDays(all);

        const result = read({ start: tsOf(2, 1), end: tsOf(3, 1) });
        assert.deepStrictEqual(tsList(result.entries), [tsOf(2, 1), tsOf(2, 2), tsOf(3, 0), tsOf(3, 1)]);
        assert.strictEqual(result.truncated, false);

        const desc = read({ start: tsOf(2, 1), end: tsOf(3, 1), sort: 'desc' });
        assert.deepStrictEqual(tsList(desc.entries), [tsOf(3, 1), tsOf(3, 0), tsOf(2, 2), tsOf(2, 1)]);

        assert.deepStrictEqual(tsList(read({}).entries), tsList(all));
        assert.deepStrictEqual(warnings, []);
    });

    it('reads only the files of the days in the range', function () {
        writeDays([entry(tsOf(2), 2), entry(tsOf(3), 3)]);
        // a broken file outside the range would be reported, if it was read
        for (const day of [1, 4]) {
            fs.mkdirSync(`${storeDir}${ts2day(tsOf(day))}`);
            fs.writeFileSync(getFilenameForID(storeDir, ts2day(tsOf(day)), ID), '[{"ts": ');
        }

        const result = read({ start: tsOf(2), end: tsOf(3) });
        assert.deepStrictEqual(tsList(result.entries), [tsOf(2), tsOf(3)]);
        assert.deepStrictEqual(warnings, []);

        read({});
        assert.strictEqual(warnings.length, 2);
    });

    it('stops after the day that reached maxEntries and keeps the first entries of the requested order', function () {
        const all = [1, 2, 3, 4, 5].flatMap(day => [0, 1, 2].map(i => entry(tsOf(day, i), day)));
        writeDays(all);

        const asc = read({ maxEntries: 4 });
        // day 2 reached 4 entries, so it is read completely and day 3 not at all
        assert.deepStrictEqual(tsList(asc.entries), tsList(all.slice(0, 6)));
        assert.strictEqual(asc.truncated, true);

        const desc = read({ maxEntries: 4, sort: 'desc' });
        assert.deepStrictEqual(tsList(desc.entries), tsList(all.slice(9).reverse()));
        assert.strictEqual(desc.truncated, true);

        // a page of a range behind the start
        const page = read({ start: tsOf(3, 2), maxEntries: 2 });
        assert.deepStrictEqual(tsList(page.entries), [tsOf(3, 2), tsOf(4, 0), tsOf(4, 1), tsOf(4, 2)]);
        assert.strictEqual(page.truncated, true);

        // exactly as many entries as the range has: complete
        assert.strictEqual(read({ maxEntries: 15 }).truncated, false);
    });

    it('is not truncated if the days that were not read have no file of the datapoint', function () {
        writeDays([entry(tsOf(1), 1), entry(tsOf(2), 2)]);
        writeDays([entry(tsOf(3), 3)], 'javascript.0.other');

        const result = read({ maxEntries: 1, start: tsOf(2) });
        assert.deepStrictEqual(tsList(result.entries), [tsOf(2)]);
        assert.strictEqual(result.truncated, false);
    });

    it('merges the entries in RAM, which replace the stored entry with the same timestamp', function () {
        writeDays([entry(tsOf(1), 1), entry(tsOf(2), 2)]);
        const cache = [entry(tsOf(2), 20), entry(tsOf(2, 5), 25), null, entry(tsOf(6), 6)];

        const result = read({ cache });
        assert.deepStrictEqual(tsList(result.entries), [tsOf(1), tsOf(2), tsOf(2, 5), tsOf(6)]);
        assert.strictEqual(result.entries[1].val, 20);

        assert.deepStrictEqual(tsList(read({ cache, end: tsOf(2) }).entries), [tsOf(1), tsOf(2)]);
    });

    it('leaves out the entries in RAM of the days that were not read', function () {
        writeDays([1, 2, 3].map(day => entry(tsOf(day), day)));
        // one of a day that was read, one of a day that was not and one of a day without file
        const cache = [entry(tsOf(1, 5), 15), entry(tsOf(3, 5), 35), entry(tsOf(7), 7)];

        const asc = read({ cache, maxEntries: 1 });
        assert.deepStrictEqual(tsList(asc.entries), [tsOf(1), tsOf(1, 5)]);
        assert.strictEqual(asc.truncated, true);

        const desc = read({ cache, maxEntries: 1, sort: 'desc' });
        assert.deepStrictEqual(tsList(desc.entries), [tsOf(7), tsOf(3, 5), tsOf(3)]);
        assert.strictEqual(desc.truncated, true);

        // not truncated: everything in RAM counts
        assert.strictEqual(read({ cache, maxEntries: 3 }).entries.length, 6);
    });

    it('converts timestamps in seconds of very old files and ignores what is no day folder', function () {
        const day = ts2day(tsOf(1));
        fs.mkdirSync(`${storeDir}${day}`);
        fs.writeFileSync(
            getFilenameForID(storeDir, day, ID),
            JSON.stringify([{ ts: tsOf(1) / 1000, val: 1 }, null, { ts: tsOf(1, 1000), val: 2 }]),
        );
        fs.mkdirSync(`${storeDir}.trash`);
        fs.writeFileSync(`${storeDir}20240102`, 'not a folder');

        const result = read({});
        assert.deepStrictEqual(tsList(result.entries), [tsOf(1), tsOf(1, 1000)]);
        assert.deepStrictEqual(warnings, []);
    });

    it('returns nothing if the directory does not exist', function () {
        const result = read({ storeDir: `${storeDir}missing/`, cache: [entry(tsOf(1), 1)] });
        assert.deepStrictEqual(tsList(result.entries), [tsOf(1)]);
        assert.strictEqual(result.truncated, false);
    });
});
