"use strict";
/**
 * Pure helpers behind the `getDpStatistics` and `cleanupOrphaned` messages.
 *
 * Kept out of `main.ts` so they can be unit tested without starting the adapter. Shared in shape
 * with the same file of ioBroker.sql and ioBroker.influxdb, so the statistics tabs of the three
 * adapters stay comparable - only the storage specific columns differ.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.classifyDatapoint = classifyDatapoint;
exports.summarize = summarize;
exports.selectForCleanup = selectForCleanup;
/**
 * Decide how a datapoint should be classified.
 *
 * @param objectExists whether the ioBroker object for this ID still exists
 * @param loggingEnabled whether this instance currently logs the ID
 */
function classifyDatapoint(objectExists, loggingEnabled) {
    if (!objectExists) {
        return 'objectMissing';
    }
    return loggingEnabled ? 'active' : 'loggingDisabled';
}
/**
 * Totals for the statistics table.
 *
 * @param stats the per-datapoint statistics
 */
function summarize(stats) {
    const byStatus = {
        active: { datapoints: 0, values: 0, bytes: 0 },
        loggingDisabled: { datapoints: 0, values: 0, bytes: 0 },
        objectMissing: { datapoints: 0, values: 0, bytes: 0 },
    };
    let values = 0;
    let bytes = 0;
    for (const stat of stats) {
        values += stat.count;
        bytes += stat.bytes;
        byStatus[stat.status].datapoints++;
        byStatus[stat.status].values += stat.count;
        byStatus[stat.status].bytes += stat.bytes;
    }
    return { datapoints: stats.length, values, bytes, byStatus };
}
/**
 * Pick the datapoints a cleanup run with this scope would remove.
 *
 * An empty scope selects `objectMissing` only. Defaulting to the safe half means a caller that
 * forgets to pass a scope deletes the data nobody can reach any more, not data someone may be
 * keeping on purpose. `active` datapoints are never selectable.
 *
 * @param stats the per-datapoint statistics
 * @param scope which statuses to include
 */
function selectForCleanup(stats, scope) {
    const includeMissing = scope?.objectMissing !== false;
    const includeDisabled = scope?.loggingDisabled === true;
    return stats.filter(stat => (stat.status === 'objectMissing' && includeMissing) ||
        (stat.status === 'loggingDisabled' && includeDisabled));
}
//# sourceMappingURL=statistics.js.map