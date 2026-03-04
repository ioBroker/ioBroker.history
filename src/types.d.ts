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