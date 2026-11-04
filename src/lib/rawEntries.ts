/**
 * Reading the entries of one datapoint exactly as they are stored, for the `getRawEntries` message of
 * the data browser: no aggregation, no interpolation, no border values and no rounding.
 *
 * Kept out of `main.ts` so it can be unit tested without starting the adapter.
 */
import fs from 'node:fs';
import { getFilenameForID, ts2day } from './getHistory';
import type { IobDataEntry } from './types';

export interface RawScanOptions {
    /** Directory the history files are stored in. It always ends with a `/` */
    storeDir: string;
    /** The ID the files are named after (the Alias-ID if one is configured) */
    id: string;
    /** The entries that are not yet written to the disk */
    cache?: (IobDataEntry | null | undefined)[] | null;
    /** Beginning of the range in ms, inclusive. Everything by default */
    start?: number;
    /** End of the range in ms, inclusive. Everything by default */
    end?: number;
    /** Order of the result, and so the order in which the days are read */
    sort: 'asc' | 'desc';
    /** Once at least this many entries were found, no further day is read */
    maxEntries: number;
    log?: { warn(message: string): void };
}

export interface RawScanResult {
    /** The entries found, one per timestamp, sorted as requested */
    entries: IobDataEntry[];
    /**
     * Not every day of the range was read, because `maxEntries` was reached. The entries are still
     * exactly the first ones of the range in the requested order, only the ones behind them are missing.
     */
    truncated: boolean;
}

function sortByTs(a: IobDataEntry, b: IobDataEntry): number {
    return a.ts < b.ts ? -1 : a.ts > b.ts ? 1 : 0;
}

function sortByTsDesc(a: IobDataEntry, b: IobDataEntry): number {
    return a.ts > b.ts ? -1 : a.ts < b.ts ? 1 : 0;
}

/**
 * Read the stored entries of one datapoint.
 *
 * Every day has its own file, so only the files of the days in the range are read. They are read in
 * the requested order and the scan stops before the next file once `maxEntries` entries were found:
 * all entries of a day file belong to that day, so the entries of the days read are exactly the first
 * ones of the result. A page near the beginning of the requested order needs only a few files this way.
 *
 * The entries that are still only in RAM are taken into account too, so a value that was just logged
 * is visible immediately. The timestamp is the key of an entry, so it also removes the duplicates
 * between RAM and disk.
 */
export function readRawEntries(options: RawScanOptions): RawScanResult {
    const { storeDir, id, start, end, sort } = options;
    const dayStart = start === undefined ? 0 : parseInt(ts2day(start), 10);
    const dayEnd = end === undefined ? 99999999 : parseInt(ts2day(end), 10);
    const inRange = (ts: number): boolean => (start === undefined || ts >= start) && (end === undefined || ts <= end);

    // A folder that is no directory has no history file in it, so `fs.existsSync` below sorts it out.
    // This spares a `stat` of every day folder for every single request.
    let names: string[] = [];
    if (fs.existsSync(storeDir)) {
        try {
            names = fs.readdirSync(storeDir);
        } catch (err) {
            options.log?.warn(`Error reading data directory ${storeDir}: ${err}`);
        }
    }
    const days = names
        .map(name => ({ name, day: parseInt(name, 10) }))
        .filter(({ day }) => !isNaN(day) && day >= dayStart && day <= dayEnd)
        .sort((a, b) => (sort === 'asc' ? a.day - b.day : b.day - a.day));

    const byTs = new Map<number, IobDataEntry>();
    let truncated = false;
    /** the last day that was read */
    let lastDay: number | undefined;

    for (const { name, day } of days) {
        const file = getFilenameForID(storeDir, name, id);
        if (!fs.existsSync(file)) {
            continue;
        }
        if (byTs.size >= options.maxEntries) {
            // this day has values too, so the result is not complete
            truncated = true;
            break;
        }

        let data: IobDataEntry[];
        try {
            data = JSON.parse(fs.readFileSync(file, 'utf8'));
        } catch (err) {
            options.log?.warn(`Cannot read file "${file}": ${err}`);
            continue;
        }
        if (!Array.isArray(data)) {
            options.log?.warn(`File "${file}" does not contain an array of entries`);
            continue;
        }
        lastDay = day;

        // very old versions stored the timestamp in seconds
        const tsCheck = new Date(Math.floor(day / 10000), 0, 1).getTime();

        for (const entry of data) {
            if (!entry) {
                continue;
            }
            if (entry.ts && entry.ts < tsCheck) {
                entry.ts *= 1000;
            }
            if (inRange(entry.ts)) {
                byTs.set(entry.ts, entry);
            }
        }
    }

    for (const entry of options.cache || []) {
        if (!entry || !inRange(entry.ts)) {
            continue;
        }
        if (truncated && lastDay !== undefined) {
            // a value of a day that was not read belongs behind the result, like the values in its file
            const day = parseInt(ts2day(entry.ts), 10);
            if (sort === 'asc' ? day > lastDay : day < lastDay) {
                continue;
            }
        }
        byTs.set(entry.ts, entry);
    }

    const entries = Array.from(byTs.values());
    entries.sort(sort === 'asc' ? sortByTs : sortByTsDesc);

    return { entries, truncated };
}
