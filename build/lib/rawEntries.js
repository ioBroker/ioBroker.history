"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.readRawEntries = readRawEntries;
/**
 * Reading the entries of one datapoint exactly as they are stored, for the `getRawEntries` message of
 * the data browser: no aggregation, no interpolation, no border values and no rounding.
 *
 * Kept out of `main.ts` so it can be unit tested without starting the adapter.
 */
const node_fs_1 = __importDefault(require("node:fs"));
const getHistory_1 = require("./getHistory");
function sortByTs(a, b) {
    return a.ts < b.ts ? -1 : a.ts > b.ts ? 1 : 0;
}
function sortByTsDesc(a, b) {
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
function readRawEntries(options) {
    const { storeDir, id, start, end, sort } = options;
    const dayStart = start === undefined ? 0 : parseInt((0, getHistory_1.ts2day)(start), 10);
    const dayEnd = end === undefined ? 99999999 : parseInt((0, getHistory_1.ts2day)(end), 10);
    const inRange = (ts) => (start === undefined || ts >= start) && (end === undefined || ts <= end);
    // A folder that is no directory has no history file in it, so `fs.existsSync` below sorts it out.
    // This spares a `stat` of every day folder for every single request.
    let names = [];
    if (node_fs_1.default.existsSync(storeDir)) {
        try {
            names = node_fs_1.default.readdirSync(storeDir);
        }
        catch (err) {
            options.log?.warn(`Error reading data directory ${storeDir}: ${err}`);
        }
    }
    const days = names
        .map(name => ({ name, day: parseInt(name, 10) }))
        .filter(({ day }) => !isNaN(day) && day >= dayStart && day <= dayEnd)
        .sort((a, b) => (sort === 'asc' ? a.day - b.day : b.day - a.day));
    const byTs = new Map();
    let truncated = false;
    /** the last day that was read */
    let lastDay;
    for (const { name, day } of days) {
        const file = (0, getHistory_1.getFilenameForID)(storeDir, name, id);
        if (!node_fs_1.default.existsSync(file)) {
            continue;
        }
        if (byTs.size >= options.maxEntries) {
            // this day has values too, so the result is not complete
            truncated = true;
            break;
        }
        let data;
        try {
            data = JSON.parse(node_fs_1.default.readFileSync(file, 'utf8'));
        }
        catch (err) {
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
            const day = parseInt((0, getHistory_1.ts2day)(entry.ts), 10);
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
//# sourceMappingURL=rawEntries.js.map