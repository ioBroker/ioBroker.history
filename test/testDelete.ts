import assert from 'node:assert';
import fs from 'node:fs';
import Module from 'node:module';
import os from 'node:os';
import path from 'node:path';
import { deleteRangeProblem, parseDeleteRequests, toTimestamp } from '../src/lib/deleteRequests';
import { getFilenameForID, ts2day } from '../src/lib/getHistory';
import type { IobDataEntry } from '../src/lib/types';

// `delete` / `deleteRange` with a missing or invalid `ts` or range fell through to "delete every value of
// the datapoint": a single `{id, state: {start, end}}`, `{id, state: {}}` or `{id, state: {ts: 0}}` reached
// `_delete` as `{ts: undefined}` (or `{ts: 0}`), which emptied the values in RAM and deleted the files of all
// days up to today. An unparsable `start` became `NaN` and deleted everything up to `end`.

const ID = 'javascript.0.test';
const OTHER = 'javascript.0.other';

/** Local noon of a day in January 2024 plus some ms, so that the day of a timestamp never depends on the time zone */
function tsOf(day: number, ms = 0): number {
    return new Date(2024, 0, day, 12, 0, 0).getTime() + ms;
}

const T = tsOf(18);
const START = tsOf(2);
const END = tsOf(3);

/** Message shapes that must be refused with an error and must not delete anything */
const BAD_MESSAGES: Record<string, unknown> = {
    'single state without ts': { id: ID, state: {} },
    'single state with ts null': { id: ID, state: { ts: null } },
    'single state with a ts that is no date': { id: ID, state: { ts: 'yesterday' } },
    'single state with ts NaN': { id: ID, state: { ts: NaN } },
    'single state with an unparsable start': { id: ID, state: { start: 'garbage', end: END } },
    'single state with only end': { id: ID, state: { end: END } },
    'single entry with only end': { id: ID, end: END },
    'single entry with an unparsable start': { id: ID, start: 'garbage' },
    'single entry with an unparsable end': { id: ID, start: START, end: 'garbage' },
    'single entry with neither ts nor range': { id: ID },
    'single entry without id': { state: { ts: T } },
    'list entry without ts': [{ id: ID, state: {} }],
    'list entry with an unparsable start': [{ id: ID, start: 'garbage' }],
    'list entry with an unparsable end': [{ id: ID, start: START, end: 'garbage' }],
    'list with one valid and one invalid entry': [
        { id: ID, state: { ts: T } },
        { id: ID, state: { start: 'garbage' } },
    ],
    'state list with an entry without ts': { id: ID, state: [T, {}] },
    'state list with an unparsable start': { id: ID, state: [{ start: 'garbage' }] },
    'ts list with a value that is no date': { id: ID, ts: [T, 'garbage'] },
    'ts list with null': { id: ID, ts: [null] },
    'no object at all': 'javascript.0.test',
};

describe('Test delete: parsing of the message', function () {
    it('takes ts from the state', function () {
        assert.deepStrictEqual(parseDeleteRequests({ id: ID, state: { ts: T } }, 0), {
            requests: [{ id: ID, range: { mode: 'one', ts: T } }],
            errors: [],
        });
    });

    it('accepts a ts of 0', function () {
        for (const message of [
            { id: ID, state: { ts: 0 } },
            { id: ID, ts: 0 },
            { id: ID, ts: [0] },
            [{ id: ID, ts: 0 }],
        ]) {
            const { requests, errors } = parseDeleteRequests(message, 1);
            assert.deepStrictEqual(errors, [], JSON.stringify(message));
            assert.deepStrictEqual(requests, [{ id: ID, range: { mode: 'one', ts: 0 } }], JSON.stringify(message));
        }
    });

    it('takes a range from the state, from the entry and from a list (the bug: {id, state: {start, end}})', function () {
        const expected = { id: ID, range: { mode: 'range', start: START, end: END } };
        for (const message of [
            { id: ID, state: { start: START, end: END } },
            { id: ID, start: START, end: END },
            [{ id: ID, start: START, end: END }],
            [{ id: ID, state: { start: START, end: END } }],
            { id: ID, state: [{ start: START, end: END }] },
        ]) {
            const { requests, errors } = parseDeleteRequests(message, 1);
            assert.deepStrictEqual(errors, [], JSON.stringify(message));
            assert.deepStrictEqual(requests, [expected], JSON.stringify(message));
        }
    });

    it('lets a range without end end now, and accepts a start of 0', function () {
        assert.deepStrictEqual(parseDeleteRequests({ id: ID, start: START }, END).requests, [
            { id: ID, range: { mode: 'range', start: START, end: END } },
        ]);
        assert.deepStrictEqual(parseDeleteRequests({ id: ID, state: { start: 0, end: 0 } }, END).requests, [
            { id: ID, range: { mode: 'range', start: 0, end: 0 } },
        ]);
    });

    it('reads date strings and numeric strings', function () {
        const { requests, errors } = parseDeleteRequests(
            [{ id: ID, start: new Date(START).toISOString(), end: String(END) }],
            0,
        );
        assert.deepStrictEqual(errors, []);
        assert.deepStrictEqual(requests, [{ id: ID, range: { mode: 'range', start: START, end: END } }]);
    });

    it('takes several entries of one datapoint', function () {
        const { requests, errors } = parseDeleteRequests(
            { id: ID, state: [T, { ts: T + 1 }, { start: START, end: END }] },
            0,
        );
        assert.deepStrictEqual(errors, []);
        assert.deepStrictEqual(
            requests.map(r => r.range),
            [
                { mode: 'one', ts: T },
                { mode: 'one', ts: T + 1 },
                { mode: 'range', start: START, end: END },
            ],
        );
    });

    it('resolves aliases', function () {
        const { requests } = parseDeleteRequests([{ id: 'alias.0.x', ts: T }], 0, id => (id === 'alias.0.x' ? ID : id));
        assert.strictEqual(requests[0].id, ID);
    });

    for (const [name, message] of Object.entries(BAD_MESSAGES)) {
        it(`refuses: ${name}`, function () {
            const { errors } = parseDeleteRequests(message, Date.now());
            assert.ok(errors.length, JSON.stringify(message));
        });
    }

    it('names what is missing', function () {
        assert.match(parseDeleteRequests({ id: ID, state: {} }, 0).errors[0], /Neither "ts" nor "start"/);
        assert.match(parseDeleteRequests({ id: ID, end: END }, 0).errors[0], /"start" is missing/);
        assert.match(parseDeleteRequests({ id: ID, state: { start: 'x' } }, 0).errors[0], /Invalid "start"/);
        assert.match(parseDeleteRequests({ id: ID, start: 1, end: 'x' }, 0).errors[0], /Invalid "end"/);
        assert.match(parseDeleteRequests({ id: ID, state: { ts: NaN } }, 0).errors[0], /Invalid "ts" NaN/);
        assert.match(parseDeleteRequests({ state: { ts: T } }, 0).errors[0], /No "id"/);
    });
});

describe('Test delete: time stamps and the check of _delete', function () {
    it('converts time stamps', function () {
        assert.strictEqual(toTimestamp(0), 0);
        assert.strictEqual(toTimestamp('0'), 0);
        assert.strictEqual(toTimestamp(T), T);
        assert.strictEqual(toTimestamp(String(T)), T);
        assert.strictEqual(toTimestamp(new Date(T).toISOString()), T);
        assert.strictEqual(toTimestamp(new Date(T)), T);
        for (const value of [undefined, null, '', ' ', 'abc', NaN, Infinity, 1e20, true, {}, [], new Date(NaN)]) {
            assert.strictEqual(toTimestamp(value), undefined, String(value));
        }
    });

    it('refuses everything but a valid ts, a valid range or an explicit "all"', function () {
        for (const range of [
            undefined,
            null,
            {},
            { ts: undefined }, // what the single {id, state: {start, end}} produced before
            { ts: T }, // the old shape without mode
            { mode: 'one' },
            { mode: 'one', ts: NaN },
            { mode: 'one', ts: '2026-11-18' },
            { mode: 'range', start: NaN, end: END },
            { mode: 'range', start: START },
            { mode: 'range', start: START, end: Infinity },
            { mode: 'everything' },
        ]) {
            assert.ok(deleteRangeProblem(range as any), JSON.stringify(range));
        }
        assert.strictEqual(deleteRangeProblem({ mode: 'one', ts: 0 }), null);
        assert.strictEqual(deleteRangeProblem({ mode: 'range', start: 0, end: END }), null);
        assert.strictEqual(deleteRangeProblem({ mode: 'all' }), null);
    });
});

// The message handlers of the adapter on real files: adapter-core is replaced, so no js-controller is needed.
// They run against `build/main.js`: the esbuild of the test runner cannot read the inline `type` imports of
// `src/main.ts`. Build first.

class FakeAdapter {
    config: Record<string, unknown> = {};
    namespace = 'history.0';
    log = { silly() {}, debug() {}, info() {}, warn() {}, error() {} };
    answers: unknown[] = [];
    constructor(public options: unknown) {}
    sendTo(_from: string, _command: string, message: unknown): void {
        this.answers.push(message);
    }
}

interface TestAdapter {
    config: { storeDir: string };
    answers: any[];
    history: Record<string, { list: IobDataEntry[]; config: Record<string, unknown>; realId: string }>;
    aliasMap: Record<string, string>;
    processMessage(msg: Record<string, unknown>): void;
    _delete(id: string, range: unknown): boolean;
}

function loadAdapterFactory(): (options: unknown) => TestAdapter {
    const stubs: Record<string, unknown> = {
        '@iobroker/adapter-core': { Adapter: FakeAdapter, getAbsoluteDefaultDataDir: () => os.tmpdir() },
    };
    const mainPath = require.resolve('../build/main.js');
    const moduleWithLoad = Module as unknown as { _load: (request: string, ...rest: unknown[]) => unknown };
    const originalLoad = moduleWithLoad._load;
    delete require.cache[mainPath];
    moduleWithLoad._load = function (this: unknown, request: string, ...rest: unknown[]): unknown {
        return Object.prototype.hasOwnProperty.call(stubs, request)
            ? stubs[request]
            : originalLoad.call(this, request, ...rest);
    };
    try {
        return require(mainPath);
    } finally {
        moduleWithLoad._load = originalLoad;
        delete require.cache[mainPath];
    }
}

function entry(ts: number, val: number): IobDataEntry {
    return { ts, val, ack: 1, q: 0 } as IobDataEntry;
}

describe('Test delete: message handlers', function () {
    let createAdapter: (options: unknown) => TestAdapter;
    let storeDir: string;

    // two values on each of the days 1 - 4, two values of ID in RAM; one day of another datapoint
    const DISK = [1, 2, 3, 4].flatMap(day => [entry(tsOf(day), day * 10), entry(tsOf(day, 1), day * 10 + 1)]);
    const RAM_TS = [Date.now() - 2000, Date.now() - 1000];

    before(function () {
        createAdapter = loadAdapterFactory();
    });

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

    /** The time stamps of a datapoint in all files, ascending */
    function onDisk(id = ID): number[] {
        const list: number[] = [];
        for (const day of fs.readdirSync(storeDir)) {
            const file = getFilenameForID(storeDir, day, id);
            if (fs.existsSync(file)) {
                list.push(...JSON.parse(fs.readFileSync(file, 'utf8')).map((e: IobDataEntry) => e.ts));
            }
        }
        return list.sort((a, b) => a - b);
    }

    /** Everything stored: all files with their content and the values in RAM */
    function snapshot(adapter: TestAdapter): string {
        const files: Record<string, string> = {};
        for (const day of fs.readdirSync(storeDir).sort()) {
            for (const file of fs.readdirSync(`${storeDir}${day}`).sort()) {
                files[`${day}/${file}`] = fs.readFileSync(`${storeDir}${day}/${file}`, 'utf8');
            }
        }
        return JSON.stringify({ files, ram: adapter.history[ID].list });
    }

    function adapterWithData(): TestAdapter {
        writeDays(DISK);
        writeDays([entry(tsOf(2), 99)], OTHER);
        const adapter = createAdapter({});
        adapter.answers = [];
        adapter.config = { storeDir };
        adapter.history[ID] = { list: RAM_TS.map(ts => entry(ts, 1)), config: {}, realId: ID };
        return adapter;
    }

    function send(adapter: TestAdapter, command: string, message: unknown): any {
        adapter.processMessage({ command, message, from: 'system.adapter.javascript.0', callback: {} });
        assert.strictEqual(adapter.answers.length, 1, `exactly one answer: ${JSON.stringify(adapter.answers)}`);
        return adapter.answers[0];
    }

    const ramTs = (adapter: TestAdapter): number[] => adapter.history[ID].list.map(e => e.ts);
    const diskTs = DISK.map(e => e.ts);

    beforeEach(function () {
        storeDir = `${fs.mkdtempSync(path.join(os.tmpdir(), 'history-delete-'))}/`.replace(/\\/g, '/');
    });

    afterEach(function () {
        fs.rmSync(storeDir, { recursive: true, force: true });
    });

    for (const command of ['delete', 'deleteRange']) {
        for (const [name, message] of Object.entries(BAD_MESSAGES)) {
            it(`${command} refuses and deletes nothing: ${name}`, function () {
                const adapter = adapterWithData();
                const before = snapshot(adapter);
                const answer = send(adapter, command, message);
                assert.strictEqual(answer.success, false, JSON.stringify(answer));
                assert.ok(
                    typeof answer.error === 'string' && answer.error.endsWith('Nothing was deleted'),
                    answer.error,
                );
                assert.strictEqual(snapshot(adapter), before);
            });
        }
    }

    it('_delete refuses a missing or invalid ts or range itself', function () {
        const adapter = adapterWithData();
        const before = snapshot(adapter);
        for (const range of [
            {},
            { ts: undefined },
            { ts: 0 },
            { start: tsOf(2), end: tsOf(3) },
            { mode: 'one', ts: NaN },
            { mode: 'range', start: NaN, end: END },
            { mode: 'range', start: START },
        ]) {
            assert.throws(() => adapter._delete(ID, range), /Nothing deleted/, JSON.stringify(range));
        }
        assert.strictEqual(snapshot(adapter), before);
    });

    it('deletes exactly one value in its day file', function () {
        const adapter = adapterWithData();
        const answer = send(adapter, 'delete', [{ id: ID, state: { ts: tsOf(2, 1) } }]);
        assert.deepStrictEqual(answer, { success: true });
        assert.deepStrictEqual(
            onDisk(),
            diskTs.filter(ts => ts !== tsOf(2, 1)),
        );
        assert.deepStrictEqual(ramTs(adapter), RAM_TS);
        assert.deepStrictEqual(onDisk(OTHER), [tsOf(2)]);
    });

    it('deletes a value in RAM and reports a single value that is not there', function () {
        const adapter = adapterWithData();
        assert.deepStrictEqual(send(adapter, 'delete', { id: ID, ts: RAM_TS[0] }), { success: true });
        assert.deepStrictEqual(ramTs(adapter), [RAM_TS[1]]);
        assert.deepStrictEqual(onDisk(), diskTs);

        adapter.answers = [];
        assert.deepStrictEqual(send(adapter, 'delete', { id: ID, state: { ts: tsOf(2, 5) } }), { success: false });
        assert.deepStrictEqual(onDisk(), diskTs);
    });

    it('deletes a ts of 0 and not everything', function () {
        const adapter = adapterWithData();
        writeDays([entry(0, 0), entry(1, 1)]);
        const answer = send(adapter, 'delete', { id: ID, state: { ts: 0 } });
        assert.deepStrictEqual(answer, { success: true });
        assert.deepStrictEqual(onDisk(), [1, ...diskTs]);
        assert.deepStrictEqual(ramTs(adapter), RAM_TS);
    });

    it('deletes a range given in the state of a single entry across day files (the bug)', function () {
        for (const command of ['delete', 'deleteRange']) {
            const adapter = adapterWithData();
            const answer = send(adapter, command, { id: ID, state: { start: tsOf(2, 1), end: tsOf(3) } });
            assert.deepStrictEqual(answer, { success: true });
            assert.deepStrictEqual(onDisk(), [tsOf(1), tsOf(1, 1), tsOf(2), tsOf(3, 1), tsOf(4), tsOf(4, 1)]);
            assert.deepStrictEqual(ramTs(adapter), RAM_TS);
            assert.deepStrictEqual(onDisk(OTHER), [tsOf(2)]);
        }
    });

    it('deletes the range of every accepted form, start and end included', function () {
        const expected = [tsOf(1), tsOf(1, 1), tsOf(4), tsOf(4, 1)];
        for (const message of [
            { id: ID, start: tsOf(2), end: tsOf(3, 1) },
            [{ id: ID, start: tsOf(2), end: tsOf(3, 1) }],
            [{ id: ID, state: { start: tsOf(2), end: tsOf(3, 1) } }],
            { id: ID, state: [{ start: tsOf(2), end: tsOf(3, 1) }] },
            [{ id: ID, start: new Date(tsOf(2)).toISOString(), end: String(tsOf(3, 1)) }],
        ]) {
            const adapter = adapterWithData();
            const answer = send(adapter, 'deleteRange', message);
            assert.deepStrictEqual(answer, { success: true }, JSON.stringify(message));
            assert.deepStrictEqual(onDisk(), expected, JSON.stringify(message));
            assert.deepStrictEqual(ramTs(adapter), RAM_TS);
            // the files of the emptied days are removed
            assert.ok(!fs.existsSync(getFilenameForID(storeDir, ts2day(tsOf(2)), ID)));
            fs.rmSync(storeDir, { recursive: true, force: true });
            fs.mkdirSync(storeDir);
        }
    });

    it('deletes a range until now if end is missing, the values in RAM included', function () {
        const adapter = adapterWithData();
        const answer = send(adapter, 'deleteRange', { id: ID, start: tsOf(4, 1) });
        assert.deepStrictEqual(answer, { success: true });
        assert.deepStrictEqual(onDisk(), diskTs.slice(0, -1));
        assert.deepStrictEqual(ramTs(adapter), []);
    });

    it('reports a single range that only had values in RAM as deleted', function () {
        const adapter = adapterWithData();
        const answer = send(adapter, 'deleteRange', { id: ID, state: { start: RAM_TS[0], end: RAM_TS[1] } });
        assert.deepStrictEqual(answer, { success: true });
        assert.deepStrictEqual(ramTs(adapter), []);
        assert.deepStrictEqual(onDisk(), diskTs);
    });

    it('deletes the entries of a list one by one, alias resolved', function () {
        const adapter = adapterWithData();
        adapter.aliasMap['alias.0.x'] = ID;
        const answer = send(adapter, 'delete', { id: 'alias.0.x', ts: [tsOf(1), String(tsOf(4, 1)), RAM_TS[1]] });
        assert.deepStrictEqual(answer, { success: true });
        assert.deepStrictEqual(
            onDisk(),
            diskTs.filter(ts => ts !== tsOf(1) && ts !== tsOf(4, 1)),
        );
        assert.deepStrictEqual(ramTs(adapter), [RAM_TS[0]]);
    });

    it('deleteAll still deletes every value of the datapoint and only of it', function () {
        const adapter = adapterWithData();
        assert.deepStrictEqual(send(adapter, 'deleteAll', [{ id: ID }]), { success: true });
        assert.deepStrictEqual(onDisk(), []);
        assert.deepStrictEqual(ramTs(adapter), []);
        assert.deepStrictEqual(onDisk(OTHER), [tsOf(2)]);

        const single = adapterWithData();
        assert.deepStrictEqual(send(single, 'deleteAll', { id: ID }), { success: true });
        assert.deepStrictEqual(onDisk(), []);
        assert.deepStrictEqual(ramTs(single), []);
    });

    it('update without ts changes nothing', function () {
        const adapter = adapterWithData();
        const before = snapshot(adapter);
        assert.deepStrictEqual(send(adapter, 'update', { id: ID, state: { val: 5 } }), { success: false });
        assert.strictEqual(snapshot(adapter), before);
    });
});
