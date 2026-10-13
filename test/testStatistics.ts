import assert from 'node:assert';
import { classifyDatapoint, selectForCleanup, summarize } from '../src/lib/statistics';
import type { DatapointStat } from '../src/lib/statistics';

/**
 * Build a statistics row, so a test only has to name what it cares about
 *
 * @param id the datapoint ID
 * @param overrides the attributes this test is about
 */
function stat(id: string, overrides: Partial<DatapointStat>): DatapointStat {
    return {
        id,
        type: 'Number',
        count: 0,
        firstTs: null,
        lastTs: null,
        bytes: 0,
        files: 0,
        status: 'active',
        ...overrides,
    };
}

describe('Test statistics', function () {
    describe('classifyDatapoint', function () {
        it('separates a deleted state from one with logging switched off', function () {
            assert.strictEqual(classifyDatapoint(true, true), 'active');
            assert.strictEqual(classifyDatapoint(true, false), 'loggingDisabled');
            assert.strictEqual(classifyDatapoint(false, false), 'objectMissing');
        });

        it('reports a state that no longer exists as missing, even while it is still logged', function () {
            assert.strictEqual(classifyDatapoint(false, true), 'objectMissing');
        });
    });

    describe('summarize', function () {
        it('adds up the values and bytes in total and per status', function () {
            const summary = summarize([
                stat('a', { count: 10, bytes: 100 }),
                stat('b', { count: 5, bytes: 50, status: 'loggingDisabled' }),
                stat('c', { count: 2, bytes: 20, status: 'objectMissing' }),
                stat('d', { count: 1, bytes: 10, status: 'objectMissing' }),
            ]);
            assert.strictEqual(summary.datapoints, 4);
            assert.strictEqual(summary.values, 18);
            assert.strictEqual(summary.bytes, 180);
            assert.deepStrictEqual(summary.byStatus.active, { datapoints: 1, values: 10, bytes: 100 });
            assert.deepStrictEqual(summary.byStatus.loggingDisabled, { datapoints: 1, values: 5, bytes: 50 });
            assert.deepStrictEqual(summary.byStatus.objectMissing, { datapoints: 2, values: 3, bytes: 30 });
        });

        it('copes with no datapoints at all', function () {
            const summary = summarize([]);
            assert.strictEqual(summary.datapoints, 0);
            assert.strictEqual(summary.values, 0);
            assert.strictEqual(summary.bytes, 0);
        });
    });

    describe('selectForCleanup', function () {
        const stats = [
            stat('active', {}),
            stat('disabled', { status: 'loggingDisabled' }),
            stat('missing', { status: 'objectMissing' }),
        ];
        const ids = (list: DatapointStat[]): string[] => list.map(item => item.id);

        it('selects only the deleted states by default', function () {
            assert.deepStrictEqual(ids(selectForCleanup(stats)), ['missing']);
            assert.deepStrictEqual(ids(selectForCleanup(stats, {})), ['missing']);
        });

        it('selects the states with logging switched off only on request', function () {
            assert.deepStrictEqual(ids(selectForCleanup(stats, { loggingDisabled: true })), ['disabled', 'missing']);
            assert.deepStrictEqual(ids(selectForCleanup(stats, { objectMissing: false, loggingDisabled: true })), [
                'disabled',
            ]);
        });

        it('never selects a logged datapoint', function () {
            assert.deepStrictEqual(
                ids(selectForCleanup([stat('a', {})], { objectMissing: true, loggingDisabled: true })),
                [],
            );
        });
    });
});
