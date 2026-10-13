/**
 * Pure helpers behind the `getDpStatistics` and `cleanupOrphaned` messages.
 *
 * Kept out of `main.ts` so they can be unit tested without starting the adapter. Shared in shape
 * with the same file of ioBroker.sql and ioBroker.influxdb, so the statistics tabs of the three
 * adapters stay comparable - only the storage specific columns differ.
 */

/** Type of a datapoint, as the GUI needs it */
export type StorageType = 'Number' | 'String' | 'Boolean';

/**
 * Why a datapoint is or is not a candidate for cleanup.
 *
 * `objectMissing` and `loggingDisabled` are deliberately kept apart. A state that no longer exists
 * in ioBroker cannot produce new values and nobody can chart it, so removing its history is safe.
 * A state that still exists but has logging switched off is a different matter: the history is
 * still reachable and may well be wanted, which is why cleanup must not treat the two as one.
 */
export type DatapointStatus = 'active' | 'loggingDisabled' | 'objectMissing';

export type DatapointStat = {
    /** The ioBroker ID the files are named after (the Alias-ID if one is configured) */
    id: string;
    /** Storage type, or null when it cannot be determined any more */
    type: StorageType | null;
    /** Number of stored values (including the ones still in RAM) */
    count: number;
    /** Timestamp of the oldest value, or null when there is none */
    firstTs: number | null;
    /** Timestamp of the newest value, or null when there is none */
    lastTs: number | null;
    /** Size of the files on the disk in bytes. In contrast to ioBroker.sql this is exact, not estimated */
    bytes: number;
    /** Number of day files the values are spread over */
    files: number;
    status: DatapointStatus;
};

/**
 * Decide how a datapoint should be classified.
 *
 * @param objectExists whether the ioBroker object for this ID still exists
 * @param loggingEnabled whether this instance currently logs the ID
 */
export function classifyDatapoint(objectExists: boolean, loggingEnabled: boolean): DatapointStatus {
    if (!objectExists) {
        return 'objectMissing';
    }
    return loggingEnabled ? 'active' : 'loggingDisabled';
}

export type StatisticsSummary = {
    datapoints: number;
    /** Number of stored values over all datapoints */
    values: number;
    /** Size of all files in bytes */
    bytes: number;
    byStatus: Record<DatapointStatus, { datapoints: number; values: number; bytes: number }>;
};

/**
 * Totals for the statistics table.
 *
 * @param stats the per-datapoint statistics
 */
export function summarize(stats: DatapointStat[]): StatisticsSummary {
    const byStatus: Record<DatapointStatus, { datapoints: number; values: number; bytes: number }> = {
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

/** Which statuses a cleanup run should remove */
export type CleanupScope = {
    /** States that no longer exist in ioBroker. Safe, and the default. */
    objectMissing?: boolean;
    /** States that still exist but are not logged. Their history may still be wanted. */
    loggingDisabled?: boolean;
};

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
export function selectForCleanup<T extends DatapointStat>(stats: T[], scope?: CleanupScope): T[] {
    const includeMissing = scope?.objectMissing !== false;
    const includeDisabled = scope?.loggingDisabled === true;

    return stats.filter(
        stat =>
            (stat.status === 'objectMissing' && includeMissing) ||
            (stat.status === 'loggingDisabled' && includeDisabled),
    );
}
