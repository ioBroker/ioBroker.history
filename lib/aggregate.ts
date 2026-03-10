'use strict';
// THIS file should be identical with sql and history adapter's one

import type { IobDataEntry } from './types';

// ─── Internal types ──────────────────────────────────────────────────────────

interface AggregatePoint {
    ts: number | null;
    val: number | null;
}

interface AggregateResultEntry {
    val: AggregatePoint;
    max: AggregatePoint;
    min: AggregatePoint;
    start: AggregatePoint;
    end: AggregatePoint;
}

type AggregateType =
    | 'onchange'
    | 'minmax'
    | 'min'
    | 'max'
    | 'average'
    | 'total'
    | 'count'
    | 'none'
    | 'percentile'
    | 'quantile'
    | 'integral'
    | 'integralTotal';

/** Minimal adapter interface used by sendResponse / sendResponseCounter */
interface AdapterLike {
    log: {
        error: (msg: string) => void;
        info: (msg: string) => void;
        debug: (msg: string) => void;
    };
    sendTo: (
        instanceName: string,
        command: string,
        message: Record<string, unknown>,
        callback: ioBroker.MessageCallbackInfo,
    ) => void;
}

interface AdapterMessage {
    from: string;
    command: string;
    callback: ioBroker.MessageCallbackInfo;
    message?: {
        options?: Record<string, unknown>;
    };
}

export interface AggregateOptions {
    start?: number;
    end?: number;
    step?: number;
    count?: number;
    limit?: number;

    maxIndex?: number;

    result?: any[];
    averageCount?: number[];
    quantileDatapoints?: number[][];
    integralDatapoints?: IobDataEntry[][];
    aggregate?: AggregateType;
    overallLength?: number;

    percentile?: number;
    quantile?: number;
    integralUnit?: number;
    integralInterpolation?: string;

    logId?: string;
    logDebug?: boolean;

    log?: (...args: any[]) => void;

    removeBorderValues?: boolean;
    round?: number;
    ignoreNull?: boolean | 0 | string;
    addId?: boolean;
    id?: string;
    index?: string | number;
    ack?: boolean;
    preAggregated?: boolean;
    sessionId?: number;
}

/** Options type with all runtime-guaranteed fields after initialization */
type RuntimeOptions = Omit<AggregateOptions, 'result'> &
    Required<
        Pick<
            AggregateOptions,
            | 'start'
            | 'end'
            | 'step'
            | 'limit'
            | 'maxIndex'
            | 'averageCount'
            | 'quantileDatapoints'
            | 'integralDatapoints'
            | 'aggregate'
            | 'overallLength'
        >
    > & {
        result: (AggregateResultEntry | undefined)[];
    };

// ─── Helpers ─────────────────────────────────────────────────────────────────

function sortByTs(a: { ts: number }, b: { ts: number }): number {
    return a.ts - b.ts;
}

function makeEmptyEntry(): AggregateResultEntry {
    return {
        val: { ts: null, val: null },
        max: { ts: null, val: null },
        min: { ts: null, val: null },
        start: { ts: null, val: null },
        end: { ts: null, val: null },
    };
}

/**
 * Get quantile value from a sorted array.
 */
function getQuantileValue(q: number, list: number[]): number {
    if (q === 0) {
        return list[0];
    }

    const index = list.length * q;
    if (Number.isInteger(index)) {
        // mean of two middle numbers
        return (list[index - 1] + list[index]) / 2;
    }
    return list[Math.ceil(index - 1)];
}

/**
 * Calculate quantile for given array of values.
 */
function quantile(qOrPs: number | number[], list: number[]): number | number[] {
    const q = Array.isArray(qOrPs) ? qOrPs : [qOrPs];

    list = list.slice().sort((a: number, b: number) => {
        a = Number.isNaN(a) ? Number.NEGATIVE_INFINITY : a;
        b = Number.isNaN(b) ? Number.NEGATIVE_INFINITY : b;

        if (a > b) {
            return 1;
        }
        if (a < b) {
            return -1;
        }
        return 0;
    });

    if (q.length === 1) {
        return getQuantileValue(q[0], list);
    }

    return q.map(qi => getQuantileValue(qi, list));
}

// ─── Core functions ──────────────────────────────────────────────────────────

function initAggregate(_options: AggregateOptions): AggregateOptions {
    const options = _options as RuntimeOptions;
    let log: (...args: unknown[]) => void = () => {};
    if (options.logDebug) {
        log = (options.log as (...args: unknown[]) => void) || console.log;
    }

    // step; // 1 Step is 1 second
    if (options.step === null || options.step === undefined) {
        options.step = (options.end - options.start) / options.count!;
    }

    // Limit 2000
    if ((options.end - options.start) / options.step > options.limit) {
        options.step = (options.end - options.start) / options.limit;
    }

    options.maxIndex = Math.ceil((options.end - options.start) / options.step - 1);
    options.result = [];
    options.averageCount = [];
    options.quantileDatapoints = [];
    options.integralDatapoints = [];
    options.aggregate = options.aggregate || 'minmax';
    options.overallLength = 0;

    if (options.aggregate === 'percentile') {
        if (typeof options.percentile !== 'number' || options.percentile < 0 || options.percentile > 100) {
            options.percentile = 50;
        }
        options.quantile = options.percentile / 100; // Internally we use quantile for percentile too
    }
    if (options.aggregate === 'quantile') {
        if (typeof options.quantile !== 'number' || options.quantile < 0 || options.quantile > 1) {
            options.quantile = 0.5;
        }
    }
    if (options.aggregate === 'integral') {
        if (typeof options.integralUnit !== 'number' || options.integralUnit <= 0) {
            options.integralUnit = 60;
        }
        options.integralUnit *= 1000; // Convert to milliseconds
    }

    log(
        `${options.logId} Initialize: maxIndex = ${options.maxIndex}, step = ${options.step}, start = ${options.start}, end = ${options.end}`,
    );
    // pre-fill the result with timestamps (add one before start and one after the end)
    try {
        options.result.length = options.maxIndex + 2;
    } catch (err) {
        (err as Error).message += `: ${options.maxIndex + 2}`;
        throw err;
    }
    // We define the array length but do not prefill values, we do that on runtime when needed
    options.result[0] = makeEmptyEntry();
    options.result[options.maxIndex + 2] = makeEmptyEntry();

    if (options.aggregate === 'average') {
        options.averageCount[0] = 0;
        options.averageCount[options.maxIndex + 2] = 0;
    }

    if (options.aggregate === 'percentile' || options.aggregate === 'quantile') {
        options.quantileDatapoints[0] = [];
        options.quantileDatapoints[options.maxIndex + 2] = [];
    }
    if (options.aggregate === 'integral') {
        options.integralDatapoints[0] = [];
        options.integralDatapoints[options.maxIndex + 2] = [];
    }
    return options;
}

function aggregationLogic(data: IobDataEntry, index: number, _options: AggregateOptions): void {
    const options = _options as RuntimeOptions;
    let log: (...args: unknown[]) => void = () => {};
    if (options.logDebug) {
        log = (options.log as (...args: unknown[]) => void) || console.log;
    }

    if (!options.result[index]) {
        log(`${options.logId} Data index ${index} not initialized, ignore!`);
        return;
    }

    const entry = options.result[index];

    if (options.aggregate !== 'minmax' && !entry.val.ts) {
        entry.val.ts = Math.round(options.start + (index - 1 + 0.5) * options.step);
    }

    if (options.aggregate === 'max') {
        if (entry.val.val === null || entry.val.val < (data.val as number)) {
            entry.val.val = data.val;
        }
    } else if (options.aggregate === 'min') {
        if (entry.val.val === null || entry.val.val > (data.val as number)) {
            entry.val.val = data.val;
        }
    } else if (options.aggregate === 'average') {
        entry.val.val = (entry.val.val ?? 0) + parseFloat(data.val as unknown as string);
        options.averageCount[index]++;
    } else if (options.aggregate === 'count') {
        options.averageCount[index]++;
    } else if (options.aggregate === 'total') {
        entry.val.val = (entry.val.val ?? 0) + parseFloat(data.val as unknown as string);
    } else if (options.aggregate === 'minmax') {
        if (entry.min.ts === null) {
            entry.min.ts = data.ts;
            entry.min.val = data.val;

            entry.max.ts = data.ts;
            entry.max.val = data.val;

            entry.start.ts = data.ts;
            entry.start.val = data.val;

            entry.end.ts = data.ts;
            entry.end.val = data.val;
        } else {
            if (data.val !== null) {
                if (data.val > entry.max.val!) {
                    entry.max.ts = data.ts;
                    entry.max.val = data.val;
                } else if (data.val < entry.min.val!) {
                    entry.min.ts = data.ts;
                    entry.min.val = data.val;
                }
                if (data.ts > entry.end.ts!) {
                    entry.end.ts = data.ts;
                    entry.end.val = data.val;
                }
            } else {
                if (data.ts > entry.end.ts!) {
                    entry.end.ts = data.ts;
                    entry.end.val = null;
                }
            }
        }
    } else if (options.aggregate === 'percentile' || options.aggregate === 'quantile') {
        options.quantileDatapoints[index].push(data.val as number);
        log(`${options.logId} Quantile ${index}: Add ts= ${data.ts} val=${data.val}`);
    } else if (options.aggregate === 'integral') {
        options.integralDatapoints[index].push(data);
        log(`${options.logId} Integral ${index}: Add ts= ${data.ts} val=${data.val}`);
    }
}

function aggregation(
    _options: AggregateOptions,
    data: IobDataEntry[],
): { result: any[]; step: number | undefined; sourceLength: number } {
    const options = _options as RuntimeOptions;
    let index: number;
    let preIndex: number;

    let collectedTooEarlyData: IobDataEntry[] = [];
    let collectedTooLateData: IobDataEntry[] = [];
    let preIndexValueFound = false;
    let postIndexValueFound = false;

    for (let i = 0; i < data.length; i++) {
        if (!data[i]) {
            continue;
        }
        if (typeof data[i].ts !== 'number') {
            data[i].ts = parseInt(data[i].ts as unknown as string, 10);
        }

        preIndex = Math.floor((data[i].ts - options.start) / options.step);

        // store all border values
        if (preIndex < 0) {
            index = 0;
            // if the ts is even earlier than the "pre-interval" ignore it, else we collect all data there
            if (preIndex < -1) {
                collectedTooEarlyData.push(data[i]);
                continue;
            }
            preIndexValueFound = true;
        } else if (preIndex > options.maxIndex) {
            index = options.maxIndex + 2;
            // if the ts is even later than the "post-interval" ignore it, else we collect all data there
            if (preIndex > options.maxIndex + 1) {
                collectedTooLateData.push(data[i]);
                continue;
            }
            postIndexValueFound = true;
        } else {
            index = preIndex + 1;
        }
        options.overallLength++;

        if (options.result[index] === undefined) {
            // lazy initialization of data structure
            options.result[index] = makeEmptyEntry();

            if (options.aggregate === 'average' || options.aggregate === 'count') {
                options.averageCount[index] = 0;
            }

            if (options.aggregate === 'percentile' || options.aggregate === 'quantile') {
                options.quantileDatapoints[index] = [];
            }
            if (options.aggregate === 'integral') {
                options.integralDatapoints[index] = [];
            }
        }

        aggregationLogic(data[i], index, options);
    }

    // If no data was found in the pre-interval, but we have earlier data, we put the latest of them in the pre-interval
    if (!preIndexValueFound && collectedTooEarlyData.length > 0) {
        collectedTooEarlyData = collectedTooEarlyData.sort(sortByTs);
        options.overallLength++;
        aggregationLogic(collectedTooEarlyData[collectedTooEarlyData.length - 1], 0, options);
    }
    // If no data was found in the post-interval, but we have later data, we put the earliest of them in the post-interval
    if (!postIndexValueFound && collectedTooLateData.length > 0) {
        collectedTooLateData = collectedTooLateData.sort(sortByTs);
        options.overallLength++;
        aggregationLogic(collectedTooLateData[0], options.maxIndex + 2, options);
    }

    return { result: options.result, step: options.step, sourceLength: data.length };
}

function finishAggregation(_options: AggregateOptions): void {
    const options = _options as RuntimeOptions;
    let log: (...args: unknown[]) => void = () => {};
    if (options.logDebug) {
        log = (options.log as (...args: unknown[]) => void) || console.log;
    }

    if (options.aggregate === 'minmax') {
        let preBorderValueRemoved = false;
        let postBorderValueRemoved = false;
        const originalResultLength = options.result.length;

        const startIndex = 0;
        const endIndex = options.result.length;
        const finalResult: IobDataEntry[] = [];

        for (let ii = startIndex; ii < endIndex; ii++) {
            const entry = options.result[ii];
            // no one value in this period
            if (entry === undefined || entry.start.ts === null) {
                if (ii === 0) {
                    preBorderValueRemoved = true;
                } else if (ii === originalResultLength - 1) {
                    postBorderValueRemoved = true;
                }
                continue;
            }
            // just one value in this period: max == min == start == end
            if (entry.start.ts === entry.end.ts) {
                finalResult.push({
                    ts: entry.start.ts,
                    val: entry.start.val,
                });
            } else if (entry.min.ts === entry.max.ts) {
                // if just 2 values: start == min == max, end
                if (entry.start.ts === entry.min.ts || entry.end.ts === entry.min.ts) {
                    finalResult.push({
                        ts: entry.start.ts,
                        val: entry.start.val,
                    });
                    finalResult.push({
                        ts: entry.end.ts!,
                        val: entry.end.val,
                    });
                } else {
                    // if just 3 values: start, min == max, end
                    finalResult.push({
                        ts: entry.start.ts,
                        val: entry.start.val,
                    });
                    finalResult.push({
                        ts: entry.max.ts!,
                        val: entry.max.val,
                    });
                    finalResult.push({
                        ts: entry.end.ts!,
                        val: entry.end.val,
                    });
                }
            } else if (entry.start.ts === entry.max.ts) {
                // just one value in this period: start == max, min == end
                if (entry.min.ts === entry.end.ts) {
                    finalResult.push({
                        ts: entry.start.ts,
                        val: entry.start.val,
                    });
                    finalResult.push({
                        ts: entry.end.ts!,
                        val: entry.end.val,
                    });
                } else {
                    // start == max, min, end
                    finalResult.push({
                        ts: entry.start.ts,
                        val: entry.start.val,
                    });
                    finalResult.push({
                        ts: entry.min.ts!,
                        val: entry.min.val,
                    });
                    finalResult.push({
                        ts: entry.end.ts!,
                        val: entry.end.val,
                    });
                }
            } else if (entry.end.ts === entry.max.ts) {
                // just one value in this period: start == min, max == end
                if (entry.min.ts === entry.start.ts) {
                    finalResult.push({
                        ts: entry.start.ts,
                        val: entry.start.val,
                    });
                    finalResult.push({
                        ts: entry.end.ts!,
                        val: entry.end.val,
                    });
                } else {
                    // start, min, max == end
                    finalResult.push({
                        ts: entry.start.ts,
                        val: entry.start.val,
                    });
                    finalResult.push({
                        ts: entry.min.ts!,
                        val: entry.min.val,
                    });
                    finalResult.push({
                        ts: entry.end.ts!,
                        val: entry.end.val,
                    });
                }
            } else if (entry.start.ts === entry.min.ts || entry.end.ts === entry.min.ts) {
                // just one value in this period: start == min, max, end
                finalResult.push({
                    ts: entry.start.ts,
                    val: entry.start.val,
                });
                finalResult.push({
                    ts: entry.max.ts!,
                    val: entry.max.val,
                });
                finalResult.push({
                    ts: entry.end.ts!,
                    val: entry.end.val,
                });
            } else {
                finalResult.push({
                    ts: entry.start.ts,
                    val: entry.start.val,
                });
                // just one value in this period: start == min, max, end
                if (entry.max.ts! > entry.min.ts!) {
                    finalResult.push({
                        ts: entry.min.ts!,
                        val: entry.min.val,
                    });
                    finalResult.push({
                        ts: entry.max.ts!,
                        val: entry.max.val,
                    });
                } else {
                    finalResult.push({
                        ts: entry.max.ts!,
                        val: entry.max.val,
                    });
                    finalResult.push({
                        ts: entry.min.ts!,
                        val: entry.min.val,
                    });
                }
                finalResult.push({
                    ts: entry.end.ts!,
                    val: entry.end.val,
                });
            }
        }
        if (options.removeBorderValues) {
            // we cut out the additional results
            if (!preBorderValueRemoved) {
                finalResult.splice(0, 1);
            }
            if (!postBorderValueRemoved) {
                finalResult.length--;
            }
        }
        options.result = finalResult as unknown as (AggregateResultEntry | undefined)[];
    } else if (options.aggregate === 'average') {
        const round = options.round || 100;
        let startIndex = 0;
        let endIndex = options.result.length;
        const finalResult: IobDataEntry[] = [];
        if (options.removeBorderValues) {
            startIndex++;
            endIndex--;
        }
        for (let k = startIndex; k < endIndex; k++) {
            if (options.result[k] !== undefined && options.result[k]!.val.ts) {
                const entry = options.result[k]!;
                finalResult.push({
                    ts: entry.val.ts!,
                    val:
                        entry.val.val !== null
                            ? Math.round((entry.val.val / options.averageCount[k]) * round) / round
                            : null,
                });
            }
        }
        options.result = finalResult as unknown as (AggregateResultEntry | undefined)[];
    } else if (options.aggregate === 'count') {
        let startIndex = 0;
        let endIndex = options.result.length;
        const finalResult: IobDataEntry[] = [];
        if (options.removeBorderValues) {
            startIndex++;
            endIndex--;
        }
        for (let k = startIndex; k < endIndex; k++) {
            if (options.result[k] !== undefined && options.result[k]!.val.ts) {
                finalResult.push({
                    ts: options.result[k]!.val.ts!,
                    val: options.averageCount[k],
                });
            }
        }
        options.result = finalResult as unknown as (AggregateResultEntry | undefined)[];
    } else if (options.aggregate === 'integral') {
        let preBorderValueRemoved = false;
        let postBorderValueRemoved = false;
        const originalResultLength = options.result.length;
        const finalResult: IobDataEntry[] = [];

        for (let k = 0; k < options.result.length; k++) {
            const indexStartTs = options.start + (k - 1) * options.step;
            const indexEndTs = indexStartTs + options.step;
            if (options.integralDatapoints[k] && options.integralDatapoints[k].length) {
                // Sort data points by ts first
                options.integralDatapoints[k].sort(sortByTs);
            }
            // Make sure that we have entries that always start at the beginning of the interval
            if (
                (!options.integralDatapoints[k] ||
                    !options.integralDatapoints[k].length ||
                    options.integralDatapoints[k][0].ts > indexStartTs) &&
                options.integralDatapoints[k - 1] &&
                options.integralDatapoints[k - 1][options.integralDatapoints[k - 1].length - 1]
            ) {
                // if the first entry of this interval started somewhere in the start of the interval, add a start entry
                // same if there is no entry at all in the timeframe, use last entry from interval before
                options.integralDatapoints[k] = options.integralDatapoints[k] || [];
                options.integralDatapoints[k].unshift({
                    ts: indexStartTs,
                    val: options.integralDatapoints[k - 1][options.integralDatapoints[k - 1].length - 1].val,
                });
                log(
                    `${options.logId} Integral: ${k}: Added start entry for interval with ts=${indexStartTs}, val=${options.integralDatapoints[k][0].val}`,
                );
            } else if (
                options.integralDatapoints[k] &&
                options.integralDatapoints[k].length &&
                options.integralDatapoints[k][0].ts > indexStartTs
            ) {
                options.integralDatapoints[k].unshift({
                    ts: indexStartTs,
                    val: options.integralDatapoints[k][0].val,
                });
                log(
                    `${options.logId} Integral: ${k}: Added start entry for interval with ts=${indexStartTs}, val=${options.integralDatapoints[k][0].val} with same value as first point in interval because no former datapoint was found`,
                );
            } else if (
                options.integralDatapoints[k] &&
                options.integralDatapoints[k].length &&
                options.integralDatapoints[k][0].ts < indexStartTs
            ) {
                // if the first entry of this interval started before the start of the interval, search for the last value before the start of the interval, add as start entry
                let preFirstIndex: number | null = null;
                for (let kk = 0; kk < options.integralDatapoints[k].length; kk++) {
                    if (options.integralDatapoints[k][kk].ts >= indexStartTs) {
                        break;
                    }
                    preFirstIndex = kk;
                }
                if (preFirstIndex !== null) {
                    options.integralDatapoints[k].splice(0, preFirstIndex, {
                        ts: indexStartTs,
                        val: options.integralDatapoints[k][preFirstIndex].val,
                    } as unknown as IobDataEntry);
                    log(
                        `${options.logId} Integral: ${k}: Remove ${preFirstIndex + 1} entries and add start entry for interval with ts=${indexStartTs}, val=${options.integralDatapoints[k][0].val}`,
                    );
                }
            }

            const point: IobDataEntry = {
                ts:
                    options.result[k] !== undefined && options.result[k]!.val.ts
                        ? options.result[k]!.val.ts!
                        : Math.round(options.start + (k - 1 + 0.5) * options.step),
                val: null,
            };

            const integralDatapoints = options.integralDatapoints[k] || [];
            const vals = integralDatapoints.map(dp => `[${dp.ts}, ${dp.val}]`);
            log(
                `${options.logId} Integral: ${k}: ${integralDatapoints.length} datapoints for interval  for ${indexStartTs} - ${indexEndTs}: ${vals.join(',')}`,
            );

            // Calculate Intervals and always calculate till the interval end (start made sure above already)
            for (let kk = 0; kk < integralDatapoints.length; kk++) {
                const valEndTs = integralDatapoints[kk + 1]
                    ? Math.min(integralDatapoints[kk + 1].ts, indexEndTs)
                    : indexEndTs;
                const valDuration = valEndTs - integralDatapoints[kk].ts;
                if (valDuration < 0) {
                    log(
                        `${options.logId} Integral: ${k}[${kk}] data do not belong to this interval, ignore ${JSON.stringify(integralDatapoints[kk])} (vs. ${valEndTs})`,
                    );
                    break;
                }
                if (valDuration === 0) {
                    log(
                        `${options.logId} Integral: ${k}[${kk}] valDuration zero, ignore ${JSON.stringify(integralDatapoints[kk])}`,
                    );
                    continue;
                }
                let valStart = parseFloat(integralDatapoints[kk].val as unknown as string) || 0;
                // End value is the next value, or if none, assume "linearity
                let valEnd =
                    parseFloat(
                        (integralDatapoints[kk + 1]
                            ? integralDatapoints[kk + 1].val
                            : options.integralDatapoints[k + 1] && options.integralDatapoints[k + 1][0]
                              ? options.integralDatapoints[k + 1][0].val
                              : valStart) as unknown as string,
                    ) || 0;
                if (options.integralInterpolation !== 'linear' || valStart === valEnd) {
                    const integralAdd = (valStart * valDuration) / options.integralUnit!;
                    // simple rectangle linear interpolation
                    log(
                        `${options.logId} Integral: ${k}[${kk}] : Add ${integralAdd} from val=${valStart} for ${valDuration}`,
                    );
                    point.val = (point.val ?? 0) + integralAdd;
                } else if ((valStart >= 0 && valEnd >= 0) || (valStart <= 0 && valEnd <= 0)) {
                    // start and end are both positive or both negative, or one is 0
                    let multiplier = 1;
                    if (valStart <= 0 && valEnd <= 0) {
                        multiplier = -1; // correct the sign at the end
                        valStart = -valStart;
                        valEnd = -valEnd;
                    }
                    const minVal = Math.min(valStart, valEnd);
                    const maxVal = Math.max(valStart, valEnd);
                    const rectPart = (minVal * valDuration) / options.integralUnit!;
                    const trianglePart = ((maxVal - minVal) * valDuration * 0.5) / options.integralUnit!;
                    const integralAdd = (rectPart + trianglePart) * multiplier;
                    log(
                        `${options.logId} Integral: ${k}[${kk}] : Add R${rectPart} + T${trianglePart} => ${integralAdd} from val=${valStart} to ${valEnd} for ${valDuration}`,
                    );
                    point.val = (point.val ?? 0) + integralAdd;
                } else {
                    // Values are on different sides of 0, so we need to find the 0 crossing
                    const zeroCrossing = Math.abs((valStart * valDuration) / (valEnd - valStart));
                    // Then calculate two linear segments, one from 0 to the crossing, and one from the crossing to the end
                    const trianglePart1 = (valStart * zeroCrossing * 0.5) / options.integralUnit!;
                    const trianglePart2 = (valEnd * (valDuration - zeroCrossing) * 0.5) / options.integralUnit!;
                    const integralAdd = trianglePart1 + trianglePart2;
                    log(
                        `${options.logId} Integral: ${k}[${kk}] : Add T${trianglePart1} + T${trianglePart2} => ${integralAdd} from val=${valStart} to ${valEnd} for ${valDuration} (zero crossing ${zeroCrossing})`,
                    );
                    point.val = (point.val ?? 0) + integralAdd;
                }
            }

            if (point.val !== null) {
                finalResult.push(point);
            } else {
                if (k === 0) {
                    preBorderValueRemoved = true;
                } else if (k === originalResultLength - 1) {
                    postBorderValueRemoved = true;
                }
            }
        }
        if (options.removeBorderValues) {
            // we cut out the additional results
            if (!preBorderValueRemoved) {
                finalResult.splice(0, 1);
            }
            if (!postBorderValueRemoved) {
                finalResult.length--;
            }
        }
        options.result = finalResult as unknown as (AggregateResultEntry | undefined)[];
    } else if (options.aggregate === 'percentile' || options.aggregate === 'quantile') {
        let startIndex = 0;
        let endIndex = options.result.length;
        const finalResult: IobDataEntry[] = [];
        if (options.removeBorderValues) {
            startIndex++;
            endIndex--;
        }
        for (let k = startIndex; k < endIndex; k++) {
            if (options.result[k] !== undefined && options.result[k]!.val.ts) {
                const point: IobDataEntry = {
                    ts: options.result[k]!.val.ts!,
                    val: quantile(options.quantile!, options.quantileDatapoints[k]) as number,
                };
                log(
                    `${options.logId} Quantile ${k} ${point.ts}: ${options.quantileDatapoints[k].join(', ')} -> ${point.val}`,
                );
                finalResult.push(point);
            }
        }
        options.result = finalResult as unknown as (AggregateResultEntry | undefined)[];
    } else {
        let startIndex = 0;
        let endIndex = options.result.length;
        const finalResult: IobDataEntry[] = [];
        if (options.removeBorderValues) {
            startIndex++;
            endIndex--;
        }
        for (let j = startIndex; j < endIndex; j++) {
            if (options.result[j] !== undefined && options.result[j]!.val.ts) {
                finalResult.push({
                    ts: options.result[j]!.val.ts!,
                    val: options.result[j]!.val.val,
                });
            }
        }
        options.result = finalResult as unknown as (AggregateResultEntry | undefined)[];
    }

    beautify(options);
}

function beautify(_options: AggregateOptions): void {
    const options = _options as RuntimeOptions;
    let log: (...args: unknown[]) => void = () => {};
    if (options.logDebug) {
        log = (options.log as (...args: unknown[]) => void) || console.log;
    }

    // After finishAggregation, result is IobDataEntry[]
    const result = options.result as unknown as IobDataEntry[];

    log(`${options.logId} Beautify: ${result.length} results`);
    let preFirstValue: IobDataEntry | null = null;
    let postLastValue: IobDataEntry | null = null;

    if (options.ignoreNull === 'true') {
        // include nulls and replace them with last value
        options.ignoreNull = true;
    } else if (options.ignoreNull === 'false') {
        // include nulls
        options.ignoreNull = false;
    } else if (options.ignoreNull === '0') {
        // include nulls and replace them with 0
        options.ignoreNull = 0;
    } else if (options.ignoreNull !== true && options.ignoreNull !== false && options.ignoreNull !== 0) {
        options.ignoreNull = false;
    }

    // process null values, remove points outside the span and find first points after end and before start
    for (let i = 0; i < result.length; i++) {
        if (options.ignoreNull !== false) {
            // if value is null
            if (result[i].val === null) {
                // null value must be replaced with last not null value
                if (options.ignoreNull === true) {
                    // remove value
                    result.splice(i, 1);
                    i--;
                    continue;
                } else {
                    // null value must be replaced with 0
                    result[i].val = options.ignoreNull as number;
                }
            }
        }

        // remove all not requested points
        if (result[i].ts < options.start) {
            preFirstValue = result[i].val !== null ? result[i] : null;
            result.splice(i, 1);
            i--;
            continue;
        }

        postLastValue = result[i].val !== null ? result[i] : null;

        if (result[i].ts > options.end) {
            result.splice(i, result.length - i);
            break;
        }
    }

    // check start and stop
    if (result.length && options.aggregate !== 'none' && !options.removeBorderValues) {
        const firstTS = result[0].ts;

        if (firstTS > options.start && !options.removeBorderValues) {
            if (preFirstValue) {
                const firstY = result[0].val;
                // if steps
                if (options.aggregate === 'onchange' || !options.aggregate) {
                    if (preFirstValue.ts !== firstTS) {
                        result.unshift({ ts: options.start, val: preFirstValue.val });
                    } else {
                        if (options.ignoreNull) {
                            result.unshift({ ts: options.start, val: firstY });
                        }
                    }
                } else {
                    if (preFirstValue.ts !== firstTS) {
                        if (firstY !== null) {
                            // interpolate
                            const y =
                                (preFirstValue.val as number) +
                                ((firstY - (preFirstValue.val as number)) * (options.start - preFirstValue.ts)) /
                                    (firstTS - preFirstValue.ts);
                            result.unshift({ ts: options.start, val: y, i: true });
                            log(
                                `${options.logId} interpolate ${y} from ${preFirstValue.val} to ${firstY} as first return value`,
                            );
                        } else {
                            result.unshift({ ts: options.start, val: null });
                        }
                    } else {
                        if (options.ignoreNull) {
                            result.unshift({ ts: options.start, val: firstY });
                        }
                    }
                }
            } else {
                if (options.ignoreNull) {
                    result.unshift({ ts: options.start, val: result[0].val });
                } else {
                    result.unshift({ ts: options.start, val: null });
                }
            }
        }

        const lastTS = result[result.length - 1].ts;
        if (lastTS < options.end && !options.removeBorderValues) {
            if (postLastValue) {
                // if steps
                if (options.aggregate === 'onchange' || !options.aggregate) {
                    // if more data following, draw line to the end of the chart
                    if (postLastValue.ts !== lastTS) {
                        result.push({ ts: options.end, val: postLastValue.val });
                    } else {
                        if (options.ignoreNull) {
                            result.push({ ts: options.end, val: postLastValue.val });
                        }
                    }
                } else {
                    if (postLastValue.ts !== lastTS) {
                        const lastY = result[result.length - 1].val;
                        if (lastY !== null) {
                            // make interpolation
                            const _y =
                                lastY +
                                (((postLastValue.val as number) - lastY) * (options.end - lastTS)) /
                                    (postLastValue.ts - lastTS);
                            result.push({ ts: options.end, val: _y, i: true });
                            log(
                                `${options.logId} interpolate ${_y} from ${lastY} to ${postLastValue.val} as last return value`,
                            );
                        } else {
                            result.push({ ts: options.end, val: null });
                        }
                    } else {
                        if (options.ignoreNull) {
                            result.push({ ts: options.end, val: postLastValue.val });
                        }
                    }
                }
            } else {
                if (options.ignoreNull) {
                    const lastY = result[result.length - 1].val;
                    // if no more data, that means do not draw line
                    result.push({ ts: options.end, val: lastY });
                } else {
                    // if no more data, that means do not draw line
                    result.push({ ts: options.end, val: null });
                }
            }
        }
    } else if (options.aggregate === 'none') {
        if (options.count && result.length > options.count) {
            result.splice(0, result.length - options.count);
        }
    }

    if (options.addId) {
        for (let i = 0; i < result.length; i++) {
            if (!result[i].id && options.id) {
                result[i].id = (options.index as string) || options.id;
            }
        }
    }

    // Write back (result reference may have changed due to splicing)
    options.result = result as unknown as (AggregateResultEntry | undefined)[];
}

function sendResponse(
    adapter: AdapterLike,
    msg: AdapterMessage,
    _options: AggregateOptions,
    data: IobDataEntry[] | string,
    startTime: number,
): void {
    const options = _options as RuntimeOptions;
    let aggregateData: { result: IobDataEntry[]; step: number | undefined; sourceLength: number };
    if (typeof data === 'string') {
        adapter.log.error(data);
        adapter.sendTo(
            msg.from,
            msg.command,
            {
                result: [],
                step: 0,
                error: data,
                sessionId: options.sessionId,
            },
            msg.callback,
        );
        return;
    }

    if (options.count && !options.start && data.length > options.count) {
        data.splice(0, data.length - options.count);
    }
    if (data[0]) {
        options.start = options.start || data[0].ts;

        if (
            !options.aggregate ||
            options.aggregate === 'onchange' ||
            options.aggregate === 'none' ||
            options.preAggregated
        ) {
            aggregateData = { result: data, step: 0, sourceLength: data.length };

            // convert ack from 0/1 to false/true
            if (options.ack && aggregateData.result) {
                for (let i = 0; i < aggregateData.result.length; i++) {
                    aggregateData.result[i].ack = !!aggregateData.result[i].ack;
                }
            }
            options.result = aggregateData.result as unknown as (AggregateResultEntry | undefined)[];

            beautify(options);

            if (options.aggregate === 'none' && options.count) {
                const result = options.result as unknown as IobDataEntry[];
                if (result.length > options.count) {
                    result.splice(0, result.length - options.count);
                    options.result = result as unknown as (AggregateResultEntry | undefined)[];
                }
            }
            aggregateData.result = options.result as unknown as IobDataEntry[];
        } else {
            initAggregate(options);
            aggregateData = aggregation(options, data) as unknown as {
                result: IobDataEntry[];
                step: number | undefined;
                sourceLength: number;
            };
            finishAggregation(options);
            aggregateData.result = options.result as unknown as IobDataEntry[];
        }

        adapter.log.debug(
            `Send: ${aggregateData.result.length} of: ${aggregateData.sourceLength} in: ${Date.now() - startTime}ms`,
        );

        adapter.sendTo(
            msg.from,
            msg.command,
            {
                result: aggregateData.result,
                step: aggregateData.step,
                error: null,
                sessionId: options.sessionId,
            },
            msg.callback,
        );
    } else {
        adapter.log.info('No Data');
        adapter.sendTo(
            msg.from,
            msg.command,
            { result: [], step: null, error: null, sessionId: options.sessionId },
            msg.callback,
        );
    }
}

function sendResponseCounter(
    adapter: AdapterLike,
    msg: AdapterMessage,
    _options: AggregateOptions,
    data: IobDataEntry[] | string,
    _startTime: number,
): void {
    const options = _options as RuntimeOptions;
    // data
    // 1586713810000	100
    // 1586713810010	200
    // 1586713810040	500
    // 1586713810050	0
    // 1586713810090	400
    // 1586713810100	0
    // 1586713810110	100
    if (typeof data === 'string') {
        adapter.log.error(data);
        adapter.sendTo(
            msg.from,
            msg.command,
            {
                result: [],
                error: data,
                sessionId: options.sessionId,
            },
            msg.callback,
        );
        return;
    }

    if (data[0] && data[1]) {
        // first | start          | afterFirst | ...... | last | end            | afterLast
        // 5     |                | 8          |  9 | 1 | 3    |                | 5
        //       | 5+(8-5/tsDiff) |            |  9 | 1 |      | 3+(5-3/tsDiff) |
        //       (9 - 6.5) + (4 - 1)

        if (data[1].ts === options.start) {
            data.splice(0, 1);
        }

        if (data[0].ts < options.start && (data[0].val as number) > (data[1].val as number)) {
            data.splice(0, 1);
        }

        // interpolate from first to start time
        if (data[0].ts < options.start) {
            const val =
                (data[0].val as number) +
                ((data[1].val as number) - (data[0].val as number)) *
                    ((options.start - data[0].ts) / (data[1].ts - data[0].ts));
            data.splice(0, 1);
            data.unshift({ ts: options.start, val, i: true });
        }

        if (data[data.length - 2] !== undefined && data[data.length - 2].ts === options.end) {
            data.length--;
        }

        const veryLast = data[data.length - 1];
        const beforeLast = data[data.length - 2];

        // interpolate from end time to last
        if (veryLast !== undefined && beforeLast !== undefined && options.end < veryLast.ts) {
            const val =
                (beforeLast.val as number) +
                ((veryLast.val as number) - (beforeLast.val as number)) *
                    ((options.end - beforeLast.ts) / (veryLast.ts - beforeLast.ts));
            data.length--;
            data.push({ ts: options.end, val, i: true });
        }

        // at this point we expect [6.5, 9, 1, 4]
        // at this point we expect [150, 200, 500, 0, 400, 0, 50]
        let sum = 0;
        if (data.length > 1) {
            let val = data[data.length - 1].val as number;
            for (let i = data.length - 2; i >= 0; i--) {
                if ((data[i].val as number) < val) {
                    sum += val - (data[i].val as number);
                }
                val = data[i].val as number;
            }
        }

        adapter.sendTo(
            msg.from,
            msg.command,
            {
                result: sum,
                error: null,
                sessionId: options.sessionId,
            },
            msg.callback,
        );
    } else {
        adapter.log.info('No Data');
        adapter.sendTo(
            msg.from,
            msg.command,
            { result: 0, step: null, error: null, sessionId: options.sessionId },
            msg.callback,
        );
    }
}

// ─── Exports ─────────────────────────────────────────────────────────────────

export { sendResponseCounter, sendResponse, initAggregate, aggregation, beautify, finishAggregation, sortByTs };

export type { AggregateResultEntry, AggregatePoint, AdapterLike, AdapterMessage };
