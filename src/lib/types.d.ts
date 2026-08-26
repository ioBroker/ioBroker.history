import type { InternalHistoryOptions as AggregateOptions } from '@iobroker/aggregate';

// The types of the `getHistory` contract and of the aggregation are shared with the sql and influxdb
// adapters, so they live in `@iobroker/aggregate` and are only re-exported here.
export type {
    AggregateMethod,
    DataEntry,
    GetHistoryOptions,
    GetHistoryOptionsExtended,
    GetStatistics,
    IobDataEntry,
    ProcessingEntry,
    SmartDate,
    TimeInterval,
} from '@iobroker/aggregate';

export interface HistoryAdapterConfig {
    maxLength: number | string;
    limit: number | string;
    storeDir: string;
    blockTime: number | string;
    debounceTime: number | string;
    retention: number | string;
    storeFrom: boolean;
    storeAck: boolean;
    changesRelogInterval: number | string;
    changesMinDelta: number | string;
    writeNulls: boolean;
    disableSkippedValueLogging: boolean;
    enableLogging: boolean;
    enableDebugLogs: boolean;
    round: number | string | null;
    customRetentionDuration: number | string;
    debounce: number | string;
}

/** The aggregation state of `@iobroker/aggregate` plus the fields that only the history adapter needs */
export interface InternalHistoryOptions extends AggregateOptions {
    id: string;
    /** Prefix of all log messages of one request, so that parallel requests can be told apart */
    logId: string;
    /** Number of entries that were found in the RAM cache */
    length?: number;
    /** Directory the history files are stored in. It always ends with a `/` */
    path: string;
    /** if `user` field should be included in answer */
    user?: boolean;
    /** if `c` (comment) field should be included in answer */
    comment?: boolean;
}
