// todo     add cache data
// todo     error tests
// todo     clean up
import * as fs from 'node:fs';
import { initAggregate, aggregation, finishAggregation } from './aggregate';
import type { IobDataEntry, InternalHistoryOptions } from './types';

type ResponseTuple = ['response', IobDataEntry[] | undefined, number | undefined, number | undefined];

// ─── Module-level state ──────────────────────────────────────────────────────

let gOptions: InternalHistoryOptions;

let finished = false;
let initialized = false;
let cacheReceived = false;
let cacheData: IobDataEntry[] | null = null;

// ─── Script entry point ───────────────────────────────────────────────────────

if (require.main === module) {
    gOptions = JSON.parse(process.argv[2]) as InternalHistoryOptions;

    if (gOptions?.logDebug) {
        gOptions.log = (...args: unknown[]): void => {
            process.send?.(['debug', ...args]);
        };
    } else {
        gOptions.log = (): void => {};
    }

    process.on('message', (message: [command: string, data: IobDataEntry[] | null]): void => {
        if (message[0] === 'cacheData') {
            cacheReceived = true;
            gOptions.log!(
                `${Date.now()}: cacheData received (cnt=${message[1] ? message[1].length : 'none'}, initialized=${initialized})`,
            );
            if (initialized) {
                gOptions.log!(`${Date.now()}: aggregate cacheData`);
                if (message[1]) {
                    aggregation(gOptions, message[1]);
                }
                gOptions.log!(`${Date.now()}: aggregate cacheData Done`);

                if (finished) {
                    response(gOptions);
                } else {
                    finished = true;
                }
            } else {
                cacheData = message[1];
            }
        } else if (message[0] === 'exit') {
            process.exit();
        }
    });

    gOptions.log!(`${Date.now()}: Initialize getHistory`);

    if (process.send) {
        gOptions.log!(`${Date.now()}: Request cacheData`);
        process.send(['getCache', gOptions], () => processData(gOptions));
    } else {
        finished = true;
        processData(gOptions);
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function getDirectories(path: string): string[] {
    return fs.readdirSync(path).filter(file => !file.startsWith('.') && fs.statSync(`${path}/${file}`).isDirectory());
}

function tsSort(a: IobDataEntry, b: IobDataEntry): number {
    return b.ts - a.ts;
}

function getFilenameForID(path: string, date: string | number, id: string | number): string {
    const safeId = id.toString().replace(/[\u0000|*,;"'<>?:/\\]/g, '~');
    return `${path}${date.toString()}/history.${safeId}.json`;
}

function ts2day(ts: number): string {
    const dateObj = new Date(ts);

    let text = dateObj.getFullYear().toString();
    let v = dateObj.getMonth() + 1;
    if (v < 10) {
        text += '0';
    }
    text += v.toString();

    v = dateObj.getDate();
    if (v < 10) {
        text += '0';
    }
    text += v.toString();

    return text;
}

// ─── Core logic ───────────────────────────────────────────────────────────────

function getFileData(options: InternalHistoryOptions): void {
    const addId = options.addId;
    const dayStart = parseInt(ts2day(options.start!), 10);
    const dayEnd = parseInt(ts2day(options.end!), 10);

    options.log!(`${Date.now()}: getFileData start ${dayStart} end ${dayEnd} in ${options.path}`);

    // get a list of directories
    let dayList = getDirectories(options.path);
    if (options.returnNewestEntries) {
        dayList = dayList.sort((a, b) => parseInt(b) - parseInt(a));
    } else {
        dayList = dayList.sort((a, b) => parseInt(a) - parseInt(b));
    }

    for (let i = 0; i < dayList.length; i++) {
        const day = parseInt(dayList[i], 10);

        if (!isNaN(day) && day >= dayStart && day <= dayEnd) {
            options.log!(`${Date.now()}: getFileData process ${day}`);
            const file = getFilenameForID(options.path, dayList[i], options.id!);
            const tsCheck = new Date(Math.floor(day / 10000), 0, 1).getTime();

            try {
                if (fs.existsSync(file)) {
                    let fileData: IobDataEntry[] | null;
                    try {
                        fileData = (JSON.parse(fs.readFileSync(file, 'utf-8')) as IobDataEntry[]).sort(tsSort);
                    } catch {
                        fileData = null;
                    }

                    if (fileData) {
                        let last = false;
                        for (let ii = 0; ii < fileData.length; ii++) {
                            // if a ts in seconds is stored, convert on the fly
                            if (fileData[ii].ts && fileData[ii].ts < tsCheck) {
                                fileData[ii].ts *= 1000;
                            }

                            if (
                                typeof fileData[ii].val === 'number' &&
                                isFinite(fileData[ii].val as number) &&
                                options.round
                            ) {
                                fileData[ii].val =
                                    Math.round((fileData[ii].val as number) * options.round) / options.round;
                            }
                            if (options.ack) {
                                fileData[ii].ack = !!fileData[ii].ack;
                            }
                            if (!options.q && fileData[ii].q !== undefined) {
                                delete fileData[ii].q;
                            }
                            if (!options.user && fileData[ii].user !== undefined) {
                                delete fileData[ii].user;
                            }
                            if (!options.comment && fileData[ii].c !== undefined) {
                                delete fileData[ii].c;
                            }
                            if (addId) {
                                fileData[ii].id = options.id;
                            }
                            if (
                                (options.returnNewestEntries ||
                                    options.aggregate === 'onchange' ||
                                    (options.aggregate as string) === '' ||
                                    options.aggregate === 'none') &&
                                ii >= (options.count ?? Infinity)
                            ) {
                                fileData.length = ii;
                                break;
                            }
                            if (last) {
                                fileData.length = ii;
                                break;
                            }
                            if (options.start && fileData[ii].ts < options.start) {
                                last = true;
                            }
                        }
                    }

                    options.log!(
                        `${Date.now()}: getFileData aggregate ${day} (length=${fileData ? fileData.length : 0})`,
                    );
                    if (fileData) {
                        aggregation(options, fileData as IobDataEntry[]);
                    }
                }
            } catch (err) {
                options.log!(`${Date.now()}: getFileData error ${day} ${(err as Error).message}`);
                options.log!(`${Date.now()}: ${(err as Error).stack}`);
            }
        }

        if (day > dayEnd) {
            break;
        }
    }
    options.log!(`${Date.now()}: getFileData Done`);
}

function response(options: InternalHistoryOptions): ResponseTuple | void {
    options.log!(`${Date.now()}: Finish Aggregation`);
    finishAggregation(options);
    options.log!(`${Date.now()}: Send Response`);

    if (require.main === module) {
        if (process.send) {
            process.send(['response', options.result, options.overallLength, options.step], () => {
                setTimeout(() => process.exit(), 200);
            });
        } else {
            setTimeout(() => process.exit(), 500);
        }
    } else {
        return ['response', options.result, options.overallLength, options.step];
    }
}

function processData(options: InternalHistoryOptions): void {
    options.log!(`${Date.now()}: Initialize structures`);
    // initAggregate mutates options in-place and returns the same object
    initAggregate(options);
    initialized = true;

    if (cacheReceived) {
        options.log!(`${Date.now()}: Aggregate cacheData`);
        if (cacheData) {
            aggregation(options, cacheData);
        }
        options.log!(`${Date.now()}: Aggregate cacheData Done`);
        cacheData = null;
        finished = true;
    }

    getFileData(options);

    if (finished) {
        response(options);
    } else {
        finished = true;
    }
}

// ─── Exports (when used as a module) ─────────────────────────────────────────

export { initAggregate, aggregation, finishAggregation, getFileData, ts2day, response, getFilenameForID };

// how to use:
// const options = initAggregate(options);
// getFileData(options);
// const result = response(options);
