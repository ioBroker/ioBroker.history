// THIS file should be identical with SQL and history adapter's one

import type { GetHistoryOptions, InternalHistoryOptions, IobDataEntry, TimeInterval } from './types';

/**
 * Calculate the square of triangle between two data points on chart
 * val  |               |
 * |    |         /-----|-----
 * |    |   /__--/######|
 * |----|--/############|
 * |    |###############|
 * +----o---------------n--->time
 * Square is the #, deltaT = x2 - x1, DeltaY = y2 - y1
 *
 * @param oldVal
 * @param newVal
 */
export function calcDiff(
    oldVal: IobDataEntry,
    newVal: IobDataEntry,
): { square: number; deltaT: number; deltaY: number } {
    // Ynew - Yold (threat null as zero)
    const diff = (newVal.val || 0) - (oldVal.val || 0);
    // (Xnew - Xold) / 3600000 (to hours)
    const deltaT = (newVal.ts - oldVal.ts) / 3600000; // ms => hours

    // if deltaT is negative, we have a problem as the time cannot go back
    if (deltaT < 0) {
        return { square: 0, deltaT: 0, deltaY: 0 };
    }

    // Yold * (Xnew - Xold) / 3600000
    let square = (oldVal.val || 0) * deltaT;

    // Yold * (Xnew - Xold) / 3600000 + (Ynew - Yold) * (Xnew - Xold) / 2
    square += (diff * deltaT) / 2;
    return { square, deltaT, deltaY: diff };
}

function interpolate2points(p1: IobDataEntry, p2: IobDataEntry, ts: number): number {
    const dx = p2.ts - p1.ts;
    // threat null as zero
    const dy = (p2.val || 0) - (p1.val || 0);
    if (!dx) {
        return p1.val!;
    }
    return (dy * (ts - p1.ts)) / dx + p1.val!;
}

export function initAggregate(
    initialOptions: GetHistoryOptions,
    timeIntervals?: TimeInterval[],
    log?: (text: string) => void,
): InternalHistoryOptions {
    const options: InternalHistoryOptions = initialOptions as InternalHistoryOptions;
    options.log = log;
    if (!options.log) {
        options.log = () => {};
        // To save the complex outputs
        options.logDebug = false;
    }

    // step; // 1 Step is 1 second
    if (options.step === null) {
        options.step = (options.end! - options.start!) / options.count!;
    }

    // Limit 2000
    if ((options.end! - options.start!) / options.step! > options.limit!) {
        options.step = (options.end! - options.start!) / options.limit!;
    }

    if (timeIntervals) {
        options.timeIntervals = timeIntervals;
        options.maxIndex = options.timeIntervals.length - 1;
    } else {
        // MaxIndex is the index, that can really called without -1
        options.maxIndex = Math.ceil((options.end! - options.start!) / options.step! - 1);
    }
    options.processing = [];
    options.result = []; // finalResult
    options.averageCount = [];
    options.quantileDataPoints = [];
    options.integralDataPoints = [];
    options.totalIntegralDataPoints = [];
    options.aggregate = options.aggregate || 'minmax';
    options.overallLength = 0;
    options.currentTimeInterval = 0;

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

    if (options.logDebug && options.log) {
        options.log(
            `Initialize: maxIndex = ${options.maxIndex}, step = ${!timeIntervals ? options.step : 'smart'}, start = ${options.start}, end = ${options.end}`,
        );
    }

    // pre-fill the result with timestamps (add one before start and one after end)
    try {
        options.processing.length = options.maxIndex + 2;
    } catch (err) {
        err.message += `: ${options.maxIndex + 2}`;
        throw err;
    }
    // We define the array length, but do not prefill values, we do that on runtime when needed
    options.processing[0] = {
        val: { ts: null, val: null },
        max: { ts: null, val: null },
        min: { ts: null, val: null },
        start: { ts: null, val: null },
        end: { ts: null, val: null },
    };
    options.processing[options.maxIndex + 2] = {
        val: { ts: null, val: null },
        max: { ts: null, val: null },
        min: { ts: null, val: null },
        start: { ts: null, val: null },
        end: { ts: null, val: null },
    };

    if (options.aggregate === 'average') {
        options.averageCount[0] = 0;
        options.averageCount[options.maxIndex + 2] = 0;
    }

    if (options.aggregate === 'percentile' || options.aggregate === 'quantile') {
        options.quantileDataPoints[0] = [];
        options.quantileDataPoints[options.maxIndex + 2] = [];
    }
    if (options.aggregate === 'integral') {
        options.integralDataPoints[0] = [];
        options.integralDataPoints[options.maxIndex + 2] = [];
    }
    return options;
}

export function aggregation(
    options: InternalHistoryOptions,
    data: IobDataEntry[],
): {
    processing: {
        val: { ts: number | null; val: number | null };
        max: { ts: number | null; val: number | null };
        min: { ts: number | null; val: number | null };
        start: { ts: number | null; val: number | null };
        end: { ts: number | null; val: number | null };
    }[];
    step: number;
    sourceLength: number;
} {
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

        if (options.timeIntervals) {
            // We have specific time intervals and collects all data according to this information
            const dataTs = data[i].ts;
            // Find a time interval for this timestamp
            if (dataTs < options.start!) {
                index = 0;
            } else if (dataTs >= options.end!) {
                index = options.timeIntervals.length + 1;
            } else if (dataTs >= options.timeIntervals[options.currentTimeInterval!].end) {
                // Look for the next interval
                options.currentTimeInterval!++;
                while (options.currentTimeInterval! < options.timeIntervals.length) {
                    if (
                        options.timeIntervals[options.currentTimeInterval!].start <= dataTs &&
                        dataTs < options.timeIntervals[options.currentTimeInterval!].end
                    ) {
                        break;
                    }
                    options.currentTimeInterval!++;
                }
                index = options.currentTimeInterval! + 1;
            } else {
                index = options.currentTimeInterval! + 1;
            }
        } else {
            // Our intervals are equidistant and we can calculate them
            preIndex = Math.floor((data[i].ts - options.start!) / options.step!);

            // store all border values
            if (preIndex < 0) {
                index = 0;
                // if the ts is even earlier than the "pre-interval" ignore it, else we collect all data there
                if (preIndex < -1) {
                    collectedTooEarlyData.push(data[i]);
                    continue;
                }
                preIndexValueFound = true;
            } else if (preIndex > options.maxIndex!) {
                index = options.maxIndex! + 2;
                // if the ts is even later than the "post-interval" ignore it, else we collect all data there
                if (preIndex > options.maxIndex! + 1) {
                    collectedTooLateData.push(data[i]);
                    continue;
                }
                postIndexValueFound = true;
            } else {
                index = preIndex + 1;
            }
            options.overallLength!++;
        }

        // Init data for time slot
        if (options.processing![index] === undefined) {
            // lazy initialization of data structure
            options.processing![index] = {
                val: { ts: null, val: null },
                max: { ts: null, val: null },
                min: { ts: null, val: null },
                start: { ts: null, val: null },
                end: { ts: null, val: null },
            };

            if (options.aggregate === 'average' || options.aggregate === 'count') {
                options.averageCount![index] = 0;
            }

            if (options.aggregate === 'percentile' || options.aggregate === 'quantile') {
                options.quantileDataPoints![index] = [];
            }
            if (options.aggregate === 'integral') {
                options.integralDataPoints![index] = [];
            }
        }

        aggregationLogic(data[i], index, options);
    }

    // If no data was found in the pre-interval, but we have earlier data, we put the latest of them in the pre-interval
    if (!preIndexValueFound && collectedTooEarlyData.length > 0) {
        collectedTooEarlyData = collectedTooEarlyData.sort(sortByTs);
        options.overallLength!++;
        aggregationLogic(collectedTooEarlyData[collectedTooEarlyData.length - 1], 0, options);
    }
    // If no data was found in the post-interval, but we have later data, we put the earliest of them in the post-interval
    if (!postIndexValueFound && collectedTooLateData.length > 0) {
        collectedTooLateData = collectedTooLateData.sort(sortByTs);
        options.overallLength!++;
        aggregationLogic(collectedTooLateData[0], options.maxIndex! + 2, options);
    }

    return { processing: options.processing!, step: options.step!, sourceLength: data.length };
}

/**
 * Execute logic for every entry in the initial series array
 *
 * @param data
 * @param index
 * @param options
 */
function aggregationLogic(data: IobDataEntry, index: number, options: InternalHistoryOptions): void {
    if (!options.processing || !options.processing[index]) {
        if (options.logDebug && options.log) {
            options.log(`Data index ${index} not initialized, ignore!`);
        }
        return;
    }

    if (options.aggregate !== 'minmax' && !options.processing[index].val.ts) {
        if (options.timeIntervals) {
            if (index === 0) {
                // If it is pre interval, make an estimation
                options.processing[0].val.ts =
                    options.timeIntervals[0].start -
                    Math.round((options.timeIntervals[0].end - options.timeIntervals[0].start) / 2);
            } else if (index > options.timeIntervals.length) {
                // If it is post interval, make an estimation
                options.processing[index].val.ts =
                    options.timeIntervals[options.timeIntervals.length - 1].end +
                    Math.round(
                        (options.timeIntervals[options.timeIntervals.length - 1].end -
                            options.timeIntervals[options.timeIntervals.length - 1].start) /
                            2,
                    );
            } else {
                // Get the middle of the interval
                options.processing[index].val.ts =
                    options.timeIntervals[index - 1].start +
                    Math.round((options.timeIntervals[index - 1].end - options.timeIntervals[index - 1].start) / 2);
            }
        } else {
            options.processing[index].val.ts = Math.round(options.start! + (index - 1 + 0.5) * options.step!);
        }
    }

    if (options.aggregate === 'max') {
        if (options.processing[index].val.val === null || options.processing[index].val.val < data.val!) {
            options.processing[index].val.val = data.val;
        }
    } else if (options.aggregate === 'min') {
        if (options.processing[index].val.val === null || options.processing[index].val.val > data.val!) {
            options.processing[index].val.val = data.val;
        }
    } else if (options.aggregate === 'average') {
        options.processing[index].val.val! += parseFloat(data.val as unknown as string);
        options.averageCount![index]++;
    } else if (options.aggregate === 'count') {
        options.averageCount![index]++;
    } else if (options.aggregate === 'total') {
        options.processing[index].val.val! += parseFloat(data.val as unknown as string);
    } else if (options.aggregate === 'minmax') {
        if (options.processing[index].min.ts === null) {
            options.processing[index].min.ts = data.ts;
            options.processing[index].min.val = data.val;

            options.processing[index].max.ts = data.ts;
            options.processing[index].max.val = data.val;

            options.processing[index].start.ts = data.ts;
            options.processing[index].start.val = data.val;

            options.processing[index].end.ts = data.ts;
            options.processing[index].end.val = data.val;
        } else {
            if (data.val !== null) {
                if (data.val > options.processing[index].max.val!) {
                    options.processing[index].max.ts = data.ts;
                    options.processing[index].max.val = data.val;
                } else if (data.val < options.processing[index].min.val!) {
                    options.processing[index].min.ts = data.ts;
                    options.processing[index].min.val = data.val;
                }
                if (data.ts > options.processing[index].end.ts!) {
                    options.processing[index].end.ts = data.ts;
                    options.processing[index].end.val = data.val;
                }
            } else {
                if (data.ts > options.processing[index].end.ts!) {
                    options.processing[index].end.ts = data.ts;
                    options.processing[index].end.val = null;
                }
            }
        }
    } else if (options.aggregate === 'percentile' || options.aggregate === 'quantile') {
        options.quantileDataPoints![index].push(data.val!);
        if (options.logDebug && options.log) {
            options.log(`Quantile ${index}: Add ts= ${data.ts} val=${data.val}`);
        }
    } else if (options.aggregate === 'integral') {
        options.integralDataPoints![index].push(data);
        if (options.logDebug && options.log) {
            options.log(`Integral ${index}: Add ts= ${data.ts} val=${data.val}`);
        }
    } else if (options.aggregate === 'integralTotal') {
        options.totalIntegralDataPoints?.push(data);
    }
}

function finishAggregationForIntegralEx(options: InternalHistoryOptions): void {
    // The first interval is pre-interval, and we need it only to calculate the first value
    // Try to find the very latest value in a pre-interval
    let index = 0;
    let workDP: IobDataEntry[];
    let next: IobDataEntry | null = null;
    let current: IobDataEntry | null = null;
    const finalResult: IobDataEntry[] = [];

    // Find first not empty interval
    // We must remember, that options.integralDataPoints is longer than options.timeIntervals on 2. It hast pre- and post- values
    do {
        workDP = options.integralDataPoints![index];
        if (workDP?.length) {
            // It must be the newest value before options.start
            current = workDP[workDP.length - 1];
            break;
        }
        index++;
    } while (index < options.integralDataPoints!.length);

    // Fill all intervals with 0
    for (let i = 0; i < options.timeIntervals!.length; i++) {
        const oneInterval: IobDataEntry = {
            ts:
                options.timeIntervals![i].start +
                Math.round(options.timeIntervals![i].end - options.timeIntervals![i].start) / 2,
            val: 0,
        };
        oneInterval.time = new Date(oneInterval.ts).toISOString();
        finalResult.push(oneInterval);
    }

    options.result = finalResult;

    if (!current) {
        return;
    }
    let workTi: TimeInterval;
    let nextIntervalIndex: number | null = null;

    // Calculate for every interval the start
    for (let i = index; i < options.timeIntervals!.length; i++) {
        workDP = options.integralDataPoints![i + 1];
        workTi = options.timeIntervals![i];

        if (workDP?.length && current) {
            // calculate the first value in this interval
            const firstValue = interpolate2points(current, workDP[0], workTi.start);
            const time: IobDataEntry = { ts: workTi.start, val: firstValue };
            if (options.logDebug && options.log) {
                time.time = new Date(time.ts).toISOString();
            }

            workDP.unshift(time);
            current = workDP[workDP.length - 1];
        }

        // Find next value
        if (nextIntervalIndex === null || nextIntervalIndex <= i) {
            next = null;
            let j = i + 1;
            do {
                if (options.integralDataPoints![j + 1]?.length) {
                    next = options.integralDataPoints![j + 1][0];
                    nextIntervalIndex = j;
                    break;
                }
                j++;
            } while (j <= options.timeIntervals!.length);
        }

        if (!next) {
            // We assume, that no more data will come
            break;
        }

        options.integralDataPoints![i + 1] = options.integralDataPoints![i + 1] || [];
        workDP = options.integralDataPoints![i + 1];

        if (!workDP.length) {
            // place first value
            // calculate the first value in this interval
            const firstValue = interpolate2points(current, next, workTi.start);
            const time: IobDataEntry = { ts: workTi.start, val: firstValue };
            if (options.logDebug && options.log) {
                time.time = new Date(time.ts).toISOString();
            }
            workDP.push(time);
        }

        // calculate the last value in this interval
        const lastValue = interpolate2points(current, next, workTi.end - 1);
        const time: IobDataEntry = { ts: workTi.end - 1, val: lastValue };
        if (options.logDebug && options.log) {
            time.time = new Date(time.ts).toISOString();
        }
        workDP.push(time);
    }

    // All intervals have start and end point, so calculate it
    for (let i = 0; i < options.timeIntervals!.length; i++) {
        workDP = options.integralDataPoints![i + 1];
        workTi = options.timeIntervals![i];
        if (!workDP) {
            continue;
        }
        finalResult[i].val = calcIntegralForPeriod(workDP);
    }
}

/**
 * This function calculates integral for one time period.
 */
function calcIntegralForPeriod(workDP: IobDataEntry[]): number {
    let sum = 0;
    for (let i = 0; i < workDP.length - 1; i++) {
        sum += calcDiff(workDP[i], workDP[i + 1]).square;
    }
    return sum;
}

function finishAggregationForIntegral(options: InternalHistoryOptions): void {
    let preBorderValueRemoved = false;
    let postBorderValueRemoved = false;
    const originalResultLength = options.processing!.length;
    const finalResult: IobDataEntry[] = [];

    if (options.timeIntervals) {
        finishAggregationForIntegralEx(options);
        return;
    }

    if (!options.processing) {
        return;
    }

    for (let k = 0; k < options.processing.length; k++) {
        let indexStartTs: number;
        let indexEndTs: number;
        /*if (options.timeIntervals) {
            if (k === 0) {
                indexEndTs = options.timeIntervals as TimeInterval[][0].start;
                indexStartTs =
                    options.timeIntervals[0].start - (options.timeIntervals[0].end - options.timeIntervals[0].start);
            } else if (k >= options.timeIntervals.length) {
                indexStartTs = options.timeIntervals[options.timeIntervals.length - 1].end;
                indexEndTs =
                    indexStartTs + (indexStartTs - options.timeIntervals[options.timeIntervals.length - 1].start);
            } else {
                indexStartTs = options.timeIntervals[k - 1].start;
                indexEndTs = options.timeIntervals[k - 1].end;
            }
        } else */ {
            indexStartTs = options.start! + (k - 1) * options.step!;
            indexEndTs = indexStartTs + options.step!;
        }

        const len = options.integralDataPoints![k]?.length;
        if (len) {
            // Sort data points by ts first
            options.integralDataPoints![k].sort(sortByTs);
        }

        // Make sure that we have entries that always start at the beginning of the interval
        if (
            (!len || options.integralDataPoints![k][0].ts > indexStartTs) &&
            options.integralDataPoints![k - 1] &&
            options.integralDataPoints![k - 1][options.integralDataPoints![k - 1].length - 1]
        ) {
            // if the first entry of this interval started somewhere in the start of the interval, add a start entry
            // same if there is no entry at all in the timeframe, use last entry from interval before
            options.integralDataPoints![k] = options.integralDataPoints![k] || [];
            const time: IobDataEntry = {
                ts: indexStartTs,
                val: options.integralDataPoints![k - 1][options.integralDataPoints![k - 1].length - 1].val,
            };
            if (options.logDebug && options.log) {
                time.time = new Date(time.ts).toISOString();
            }
            options.integralDataPoints![k].unshift(time);
            if (options.logDebug && options.log) {
                options.log(
                    `Integral: ${k}: Added start entry for interval with ts=${indexStartTs}, val=${options.integralDataPoints![k][0].val}`,
                );
            }
        } else if (len && options.integralDataPoints![k][0].ts > indexStartTs) {
            const time: IobDataEntry = {
                ts: indexStartTs,
                val: options.integralDataPoints![k][0].val,
            };
            if (options.logDebug && options.log) {
                time.time = new Date(time.ts).toISOString();
            }
            options.integralDataPoints![k].unshift(time);
            if (options.logDebug && options.log) {
                options.log(
                    `Integral: ${k}: Added start entry for interval with ts=${indexStartTs}, val=${options.integralDataPoints![k][0].val} with same value as first point in interval because no former datapoint was found`,
                );
            }
        } else if (len && options.integralDataPoints![k][0].ts < indexStartTs) {
            // if the first entry of this interval started before the start of the interval, search for the last value before the start of the interval, add as start entry
            let preFirstIndex = null;
            for (let kk = 0; kk < options.integralDataPoints![k].length; kk++) {
                if (options.integralDataPoints![k][kk].ts >= indexStartTs) {
                    break;
                }
                preFirstIndex = kk;
            }
            if (preFirstIndex !== null) {
                const time: IobDataEntry = {
                    ts: indexStartTs,
                    val: options.integralDataPoints![k][preFirstIndex].val,
                };
                if (options.logDebug && options.log) {
                    time.time = new Date(time.ts).toISOString();
                }

                options.integralDataPoints![k].splice(0, preFirstIndex, time);
                if (options.logDebug && options.log) {
                    options.log(
                        `Integral: ${k}: Remove ${preFirstIndex + 1} entries and add start entry for interval with ts=${indexStartTs}, val=${options.integralDataPoints![k][0].val}`,
                    );
                }
            }
        }

        // get middle of the time interval
        const ts: number =
            options.processing[k] !== undefined && options.processing[k].val.ts
                ? (options.processing[k].val.ts as number)
                : Math.round(indexStartTs + (indexEndTs - indexStartTs) / 2);

        const point: IobDataEntry = {
            ts,
            val: null,
        };

        const integralDataPoints = options.integralDataPoints![k] || [];
        if (options.logDebug && options.log) {
            const vals = integralDataPoints.map(dp => `[${dp.ts}, ${dp.val}]`);
            options.log(
                `Integral: ${k}: ${integralDataPoints.length} data points for interval ${indexStartTs} - ${indexEndTs}: ${vals.join(',')}`,
            );
        }

        // Calculate Intervals and always calculate till the interval end (start made sure above already)
        for (let kk = 0; kk < integralDataPoints.length; kk++) {
            const valEndTs = integralDataPoints[kk + 1]
                ? Math.min(integralDataPoints[kk + 1].ts, indexEndTs)
                : indexEndTs;

            const valDuration = valEndTs - integralDataPoints[kk].ts;
            if (valDuration < 0) {
                if (options.logDebug && options.log) {
                    options.log(
                        `Integral: ${k}[${kk}] data do not belong to this interval, ignore ${JSON.stringify(integralDataPoints[kk])} (vs. ${valEndTs})`,
                    );
                }
                break;
            }
            if (valDuration === 0) {
                if (options.logDebug && options.log) {
                    options.log(
                        `Integral: ${k}[${kk}] valDuration zero, ignore ${JSON.stringify(integralDataPoints[kk])}`,
                    );
                }
                continue;
            }
            let valStart = parseFloat(integralDataPoints[kk].val as unknown as string) || 0;
            // End value is the next value, or if none, assume "linearity"
            let valEnd =
                parseFloat(
                    (integralDataPoints[kk + 1]
                        ? integralDataPoints[kk + 1].val
                        : options.integralDataPoints![k + 1] && options.integralDataPoints![k + 1][0]
                          ? options.integralDataPoints![k + 1][0].val
                          : valStart) as unknown as string,
                ) || 0;

            if (options.integralInterpolation !== 'linear' || valStart === valEnd) {
                const integralAdd = (valStart * valDuration) / options.integralUnit!;
                // simple rectangle linear interpolation
                if (options.logDebug && options.log) {
                    options.log(`Integral: ${k}[${kk}] : Add ${integralAdd} from val=${valStart} for ${valDuration}`);
                }
                point.val! += integralAdd;
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
                if (options.logDebug && options.log) {
                    options.log(
                        `Integral: ${k}[${kk}] : Add R${rectPart} + T${trianglePart} => ${integralAdd} from val=${valStart} to ${valEnd} for ${valDuration}`,
                    );
                }
                point.val! += integralAdd;
            } else {
                // Values are on different sides of 0, so we need to find the 0 crossing
                const zeroCrossing = Math.abs((valStart * valDuration) / (valEnd - valStart));
                // Then calculate two linear segments, one from 0 to the crossing, and one from the crossing to the end
                const trianglePart1 = (valStart * zeroCrossing * 0.5) / options.integralUnit!;
                const trianglePart2 = (valEnd * (valDuration - zeroCrossing) * 0.5) / options.integralUnit!;
                const integralAdd = trianglePart1 + trianglePart2;
                if (options.logDebug && options.log) {
                    options.log(
                        `Integral: ${k}[${kk}] : Add T${trianglePart1} + T${trianglePart2} => ${integralAdd} from val=${valStart} to ${valEnd} for ${valDuration} (zero crossing ${zeroCrossing})`,
                    );
                }
                point.val! += integralAdd;
            }
        }
        /*
        options.processing[k] = {
            ts: options.processing[k].val.ts,
            val: options.processing[k].val.val
        }
        */
        if (point.val !== null) {
            finalResult.push(point);
        } else if (k === 0) {
            preBorderValueRemoved = true;
        } else if (k === originalResultLength - 1) {
            postBorderValueRemoved = true;
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

    options.result = finalResult;
}

function finishAggregationForMinMax(options: InternalHistoryOptions): void {
    if (!options.processing) {
        return;
    }

    let preBorderValueRemoved = false;
    let postBorderValueRemoved = false;
    const originalResultLength = options.processing.length;

    const startIndex = 0;
    const endIndex = options.processing.length;
    const finalResult: IobDataEntry[] = [];

    for (let ii = startIndex; ii < endIndex; ii++) {
        // it is no one value in this period
        if (options.processing[ii] === undefined || options.processing[ii].start.ts === null) {
            if (ii === 0) {
                preBorderValueRemoved = true;
            } else if (ii === originalResultLength - 1) {
                postBorderValueRemoved = true;
            }
            // options.processing.splice(ii, 1);
            continue;
        }
        // just one value in this period: max == min == start == end
        if (options.processing[ii].start.ts === options.processing[ii].end.ts) {
            finalResult.push({
                ts: options.processing[ii].start.ts as number,
                val: options.processing[ii].start.val,
            });
        } else if (options.processing[ii].min.ts === options.processing[ii].max.ts) {
            // if just 2 values: start == min == max, end
            if (
                options.processing[ii].start.ts === options.processing[ii].min.ts ||
                options.processing[ii].end.ts === options.processing[ii].min.ts
            ) {
                finalResult.push({
                    ts: options.processing[ii].start.ts as number,
                    val: options.processing[ii].start.val,
                });
                finalResult.push({
                    ts: options.processing[ii].end.ts as number,
                    val: options.processing[ii].end.val,
                });
            } else {
                // if just 3 values: start, min == max, end
                finalResult.push({
                    ts: options.processing[ii].start.ts as number,
                    val: options.processing[ii].start.val,
                });
                finalResult.push({
                    ts: options.processing[ii].max.ts as number,
                    val: options.processing[ii].max.val,
                });
                finalResult.push({
                    ts: options.processing[ii].end.ts as number,
                    val: options.processing[ii].end.val,
                });
            }
        } else if (options.processing[ii].start.ts === options.processing[ii].max.ts) {
            // just one value in this period: start == max, min == end
            if (options.processing[ii].min.ts === options.processing[ii].end.ts) {
                finalResult.push({
                    ts: options.processing[ii].start.ts as number,
                    val: options.processing[ii].start.val,
                });
                finalResult.push({
                    ts: options.processing[ii].end.ts as number,
                    val: options.processing[ii].end.val,
                });
            } else {
                // start == max, min, end
                finalResult.push({
                    ts: options.processing[ii].start.ts as number,
                    val: options.processing[ii].start.val,
                });
                finalResult.push({
                    ts: options.processing[ii].min.ts as number,
                    val: options.processing[ii].min.val,
                });
                finalResult.push({
                    ts: options.processing[ii].end.ts as number,
                    val: options.processing[ii].end.val,
                });
            }
        } else if (options.processing[ii].end.ts === options.processing[ii].max.ts) {
            // just one value in this period: start == min, max == end
            if (options.processing[ii].min.ts === options.processing[ii].start.ts) {
                finalResult.push({
                    ts: options.processing[ii].start.ts as number,
                    val: options.processing[ii].start.val,
                });
                finalResult.push({
                    ts: options.processing[ii].end.ts as number,
                    val: options.processing[ii].end.val,
                });
            } else {
                // start, min, max == end
                finalResult.push({
                    ts: options.processing[ii].start.ts as number,
                    val: options.processing[ii].start.val,
                });
                finalResult.push({
                    ts: options.processing[ii].min.ts as number,
                    val: options.processing[ii].min.val,
                });
                finalResult.push({
                    ts: options.processing[ii].end.ts as number,
                    val: options.processing[ii].end.val,
                });
            }
        } else if (
            options.processing[ii].start.ts === options.processing[ii].min.ts ||
            options.processing[ii].end.ts === options.processing[ii].min.ts
        ) {
            // just one value in this period: start == min, max, end
            finalResult.push({
                ts: options.processing[ii].start.ts as number,
                val: options.processing[ii].start.val,
            });
            finalResult.push({
                ts: options.processing[ii].max.ts as number,
                val: options.processing[ii].max.val,
            });
            finalResult.push({
                ts: options.processing[ii].end.ts as number,
                val: options.processing[ii].end.val,
            });
        } else {
            finalResult.push({
                ts: options.processing[ii].start.ts as number,
                val: options.processing[ii].start.val,
            });
            // just one value in this period: start == min, max, end
            if ((options.processing[ii].max.ts as number) > (options.processing[ii].min.ts as number)) {
                finalResult.push({
                    ts: options.processing[ii].min.ts as number,
                    val: options.processing[ii].min.val,
                });
                finalResult.push({
                    ts: options.processing[ii].max.ts as number,
                    val: options.processing[ii].max.val,
                });
            } else {
                finalResult.push({
                    ts: options.processing[ii].max.ts as number,
                    val: options.processing[ii].max.val,
                });
                finalResult.push({
                    ts: options.processing[ii].min.ts as number,
                    val: options.processing[ii].min.val,
                });
            }
            finalResult.push({
                ts: options.processing[ii].end.ts as number,
                val: options.processing[ii].end.val,
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
    options.result = finalResult;
}

function finishAggregationForAverage(options: InternalHistoryOptions): void {
    const round = options.round || 100;
    let startIndex = 0;
    if (!options.processing) {
        return;
    }
    let endIndex = options.processing.length;
    const finalResult: IobDataEntry[] = [];
    if (options.removeBorderValues) {
        // we cut out the additional results
        // options.processing.splice(0, 1);
        // options.averageCount.splice(0, 1);
        // options.processing.length--;
        // options.averageCount.length--;
        startIndex++;
        endIndex--;
    }
    for (let k = startIndex; k < endIndex; k++) {
        if (options.processing[k] !== undefined && options.processing[k].val.ts) {
            finalResult.push({
                ts: options.processing[k].val.ts as number,
                val:
                    options.processing[k].val.val !== null
                        ? Math.round(((options.processing[k].val.val as number) / options.averageCount![k]) * round) /
                          round
                        : null,
            });
        } else {
            // no one value in this interval
            // options.processing.splice(k, 1);
            // options.averageCount.splice(k, 1); // not needed to clean up because not used anymore afterwards
        }
    }
    options.result = finalResult;
}

function finishAggregationForCount(options: InternalHistoryOptions): void {
    let startIndex = 0;
    if (!options.processing) {
        return;
    }
    let endIndex = options.processing.length;
    const finalResult: IobDataEntry[] = [];
    if (options.removeBorderValues) {
        // we cut out the additional results
        // options.processing.splice(0, 1);
        // options.averageCount.splice(0, 1);
        // options.processing.length--;
        // options.averageCount.length--;
        startIndex++;
        endIndex--;
    }
    for (let k = startIndex; k < endIndex; k++) {
        if (options.processing[k] !== undefined && options.processing[k].val.ts) {
            finalResult.push({
                ts: options.processing[k].val.ts as number,
                val: options.averageCount![k],
            });
        } else {
            // no one value in this interval
            // options.processing.splice(k, 1);
            // options.averageCount.splice(k, 1); // not needed to clean up because not used anymore afterward
        }
    }
    options.result = finalResult;
}

export function finishAggregationPercentile(options: InternalHistoryOptions): void {
    let startIndex = 0;
    if (!options.processing) {
        return;
    }
    let endIndex = options.processing.length;
    const finalResult: IobDataEntry[] = [];
    if (options.removeBorderValues) {
        // we cut out the additional results
        /*
        options.processing.splice(0, 1);
        options.quantileDataPoints.splice(0, 1);
        options.processing.length--
        options.quantileDataPoints.length--;
        */
        startIndex++;
        endIndex--;
    }
    for (let k = startIndex; k < endIndex; k++) {
        if (options.processing[k] !== undefined && options.processing[k].val.ts) {
            const point: IobDataEntry = {
                ts: options.processing[k].val.ts as number,
                val: quantile(options.quantile!, options.quantileDataPoints![k]),
            };
            if (options.logDebug && options.log) {
                options.log(`Quantile ${k} ${point.ts}: ${options.quantileDataPoints![k].join(', ')} -> ${point.val}`);
            }
            finalResult.push(point);
        } else {
            // no one value in this interval
            // options.processing.splice(k, 1);
            // options.quantileDataPoints.splice(k, 1); // not needed to clean up because not used anymore afterward
        }
    }
    options.result = finalResult;
}

function finishAggregationTotalIntegral(options: InternalHistoryOptions): void {
    // calculate first entry
    if (options.totalIntegralDataPoints?.[0] && options.totalIntegralDataPoints[0].ts !== options.start) {
        if (
            options.totalIntegralDataPoints[0] &&
            options.totalIntegralDataPoints[0].ts < options.start! &&
            options.totalIntegralDataPoints[1] &&
            options.totalIntegralDataPoints[1].ts > options.start!
        ) {
            const y1 = options.totalIntegralDataPoints[0].val!;
            const y2 = options.totalIntegralDataPoints[1].val!;
            const x1 = options.totalIntegralDataPoints[0].ts;
            const x2 = options.totalIntegralDataPoints[1].ts;
            const val = y1 + ((y2 - y1) * (options.start! - x1)) / (x2 - x1);
            options.totalIntegralDataPoints[0] = {
                ts: options.start!,
                val,
            };
        }
    }

    // calculate last entry
    const len = options.totalIntegralDataPoints?.length || 0;
    if (
        options.totalIntegralDataPoints &&
        options.totalIntegralDataPoints[len - 1] &&
        options.totalIntegralDataPoints[len - 1].ts !== options.end
    ) {
        if (
            options.totalIntegralDataPoints[len - 1] &&
            options.totalIntegralDataPoints[len - 1].ts > options.end! &&
            options.totalIntegralDataPoints[len - 2] &&
            options.totalIntegralDataPoints[len - 2].ts < options.end!
        ) {
            const y1 = options.totalIntegralDataPoints[len - 2].val as number;
            const y2 = options.totalIntegralDataPoints[len - 1].val as number;
            const x1 = options.totalIntegralDataPoints[len - 2].ts;
            const x2 = options.totalIntegralDataPoints[len - 1].ts;
            const val = y1 + ((y2 - y1) * (options.start! - x1)) / (x2 - x1);
            options.totalIntegralDataPoints[len - 1] = {
                ts: options.end as number,
                val,
            };
        } else if (
            options.totalIntegralDataPoints[len - 1] &&
            options.totalIntegralDataPoints[len - 1].ts < options.end!
        ) {
            // assume that now we have the same value as before
            options.totalIntegralDataPoints.push({
                ts: options.end!,
                val: options.totalIntegralDataPoints[len - 1].val,
            });
        }
    }

    let integral = 0;
    if (options.totalIntegralDataPoints) {
        for (let i = 1; i < options.totalIntegralDataPoints.length; i++) {
            integral += calcDiff(options.totalIntegralDataPoints[i - 1], options.totalIntegralDataPoints[i]).square;
        }
    }
    options.result = [
        {
            ts: options.end!,
            val: integral,
        },
    ];
}

function finishAggregationForSimple(options: InternalHistoryOptions): void {
    let startIndex = 0;
    if (!options.processing) {
        return;
    }
    let endIndex = options.processing.length;
    const finalResult: IobDataEntry[] = [];
    if (options.removeBorderValues) {
        // we cut out the additional results
        // options.processing.splice(0, 1);
        // options.processing.length--;
        startIndex++;
        endIndex--;
    }
    for (let j = startIndex; j < endIndex; j++) {
        if (options.processing[j] !== undefined && options.processing[j].val.ts) {
            finalResult.push({
                ts: options.processing[j].val.ts as number,
                val: options.processing[j].val.val,
            });
        } else {
            // no one value in this interval
            // options.processing.splice(j, 1);
        }
    }
    options.result = finalResult;
}

export function finishAggregation(options: InternalHistoryOptions): void {
    if (options.aggregate === 'minmax') {
        finishAggregationForMinMax(options);
    } else if (options.aggregate === 'average') {
        finishAggregationForAverage(options);
    } else if (options.aggregate === 'count') {
        finishAggregationForCount(options);
    } else if (options.aggregate === 'integral') {
        finishAggregationForIntegral(options);
    } else if (options.aggregate === 'percentile' || options.aggregate === 'quantile') {
        finishAggregationPercentile(options);
    } else if (options.aggregate === 'integralTotal') {
        finishAggregationTotalIntegral(options);
    } else {
        finishAggregationForSimple(options);
    }
    // free memory
    // @ts-expect-error ignore
    options.processing = null;

    beautify(options);
}

export function beautify(options: InternalHistoryOptions): void {
    if (options.logDebug && options.log) {
        options.log(`Beautify: ${options.result?.length} results`);
    }
    let preFirstValue = null;
    let postLastValue = null;

    if ((options.ignoreNull as unknown as string) === 'true') {
        // include nulls and replace them with last value
        options.ignoreNull = true;
    } else if ((options.ignoreNull as unknown as string) === 'false') {
        // include nulls
        options.ignoreNull = false;
    } else if ((options.ignoreNull as unknown as string) === '0') {
        // include nulls and replace them with 0
        options.ignoreNull = 0;
    } else if (options.ignoreNull !== true && options.ignoreNull !== false && options.ignoreNull !== 0) {
        options.ignoreNull = false;
    }

    // process null values, remove points outside the span and find first points after end and before start
    if (options.result) {
        for (let i = 0; i < options.result.length; i++) {
            if (options.ignoreNull !== false) {
                // if value is null
                if (options.result[i].val === null) {
                    // null value must be replaced with last not null value
                    if (options.ignoreNull === true) {
                        // remove value
                        options.result.splice(i, 1);
                        i--;
                        continue;
                    } else {
                        // null value must be replaced with 0
                        options.result[i].val = options.ignoreNull;
                    }
                }
            }

            // remove all not requested points
            if (options.result[i].ts < options.start!) {
                preFirstValue = options.result[i].val !== null ? options.result[i] : null;
                options.result.splice(i, 1);
                i--;
                continue;
            }

            postLastValue = options.result[i].val !== null ? options.result[i] : null;

            if (options.result[i].ts > options.end!) {
                options.result.splice(i, options.result.length - i);
                break;
            }
            if (options.round && options.result[i].val && typeof options.result[i].val === 'number') {
                options.result[i].val = Math.round(options.result[i].val! * options.round) / options.round;
            }
        }
    }

    // check start and stop
    if (options.result?.length && options.aggregate !== 'none' && !options.removeBorderValues) {
        const firstTS = options.result[0].ts;

        if (firstTS > options.start! && !options.removeBorderValues) {
            if (preFirstValue) {
                const firstY = options.result[0].val;
                // if steps
                if (options.aggregate === 'onchange' || !options.aggregate) {
                    if (preFirstValue.ts !== firstTS) {
                        options.result.unshift({ ts: options.start!, val: preFirstValue.val });
                    } else {
                        if (options.ignoreNull) {
                            options.result.unshift({ ts: options.start!, val: firstY });
                        }
                    }
                } else {
                    if (preFirstValue.ts !== firstTS) {
                        if (firstY !== null) {
                            // interpolate
                            const y =
                                preFirstValue.val! +
                                ((firstY - preFirstValue.val!) * (options.start! - preFirstValue.ts)) /
                                    (firstTS - preFirstValue.ts);
                            options.result.unshift({ ts: options.start!, val: y, i: true });
                            if (options.logDebug && options.log) {
                                options.log(
                                    `interpolate ${y} from ${preFirstValue.val} to ${firstY} as first return value`,
                                );
                            }
                        } else {
                            options.result.unshift({ ts: options.start!, val: null });
                        }
                    } else {
                        if (options.ignoreNull) {
                            options.result.unshift({ ts: options.start!, val: firstY });
                        }
                    }
                }
            } else {
                if (options.ignoreNull) {
                    options.result.unshift({ ts: options.start!, val: options.result[0].val });
                } else {
                    options.result.unshift({ ts: options.start!, val: null });
                }
            }
        }

        const lastTS = options.result[options.result.length - 1].ts;
        if (lastTS < options.end! && !options.removeBorderValues) {
            if (postLastValue) {
                // if steps
                if (options.aggregate === 'onchange' || !options.aggregate) {
                    // if more data following, draw line to the end of the chart
                    if (postLastValue.ts !== lastTS) {
                        options.result.push({ ts: options.end!, val: postLastValue.val });
                    } else {
                        if (options.ignoreNull) {
                            options.result.push({ ts: options.end!, val: postLastValue.val });
                        }
                    }
                } else {
                    if (postLastValue.ts !== lastTS) {
                        const lastY = options.result[options.result.length - 1].val;
                        if (lastY !== null) {
                            // make interpolation
                            const _y =
                                lastY +
                                ((postLastValue.val! - lastY) * (options.end! - lastTS)) / (postLastValue.ts - lastTS);
                            options.result.push({ ts: options.end!, val: _y, i: true });
                            if (options.logDebug && options.log) {
                                options.log(
                                    `interpolate ${_y} from ${lastY} to ${postLastValue.val} as last return value`,
                                );
                            }
                        } else {
                            options.result.push({ ts: options.end!, val: null });
                        }
                    } else {
                        if (options.ignoreNull) {
                            options.result.push({ ts: options.end!, val: postLastValue.val });
                        }
                    }
                }
            } else {
                if (options.ignoreNull) {
                    const lastY = options.result[options.result.length - 1].val;
                    // if no more data, that means do not draw line
                    options.result.push({ ts: options.end!, val: lastY });
                } else {
                    // if no more data, that means do not draw line
                    options.result.push({ ts: options.end!, val: null });
                }
            }
        }
    } else if (options.aggregate === 'none') {
        if (options.count && options.result && options.result.length > options.count) {
            options.result.splice(0, options.result.length - options.count);
        }
    }

    if (options.addId && options.result) {
        for (let i = 0; i < options.result.length; i++) {
            if (!options.result[i].id && options.id) {
                options.result[i].id = options.id;
            }
        }
    }
}

export function sendResponse(
    adapter: ioBroker.Adapter,
    msg: ioBroker.Message,
    initialOptions: GetHistoryOptions,
    data: IobDataEntry[],
    startTime: number,
    logId?: string,
): void {
    if (typeof data === 'string') {
        adapter.log.error(data);

        return adapter.sendTo(
            msg.from,
            msg.command,
            {
                result: [],
                step: 0,
                error: data,
                sessionId: initialOptions.sessionId,
            },
            msg.callback,
        );
    }

    let log: ((text: string) => void) | undefined;
    if (logId) {
        log = (text: string): void => {
            adapter.log.debug(`${logId}: ${text}`);
        };
    }

    if (initialOptions.count && !initialOptions.start && data.length > initialOptions.count) {
        data.splice(0, data.length - initialOptions.count);
    }
    if (data[0]) {
        let result: IobDataEntry[];
        initialOptions.start = initialOptions.start || data[0].ts;

        let step = initialOptions.step || 0;
        const sourceLength = data.length;
        if (
            !initialOptions.aggregate ||
            initialOptions.aggregate === 'none' ||
            // @ts-expect-error clear, what is that
            initialOptions.preAggregated // TODO: What is this?
        ) {
            const options: InternalHistoryOptions = initAggregate(initialOptions, undefined, log);
            options.result = data;
            step = 0;
            // convert ack from 0/1 to false/true
            if (initialOptions.ack) {
                for (let i = 0; i < data.length; i++) {
                    data[i].ack = !!data[i].ack;
                }
            }
            beautify(options);

            if (options.aggregate === 'none' && options.count && options.result.length > options.count) {
                options.result.splice(0, options.result.length - options.count);
            }
            result = options.result;
        } else {
            const options: InternalHistoryOptions = initAggregate(initialOptions, undefined, log);
            aggregation(options, data);
            finishAggregation(options);
            result = options.result!;
        }

        adapter.log.debug(`Send: ${result.length} of: ${sourceLength} in: ${Date.now() - startTime}ms`);

        adapter.sendTo(
            msg.from,
            msg.command,
            {
                result,
                step,
                error: null,
                sessionId: initialOptions.sessionId,
            },
            msg.callback,
        );
    } else {
        adapter.log.info('No Data');
        adapter.sendTo(
            msg.from,
            msg.command,
            { result: [], step: null, error: null, sessionId: initialOptions.sessionId },
            msg.callback,
        );
    }
}

export function sendResponseCounter(
    adapter: ioBroker.Adapter,
    msg: ioBroker.Message,
    options: GetHistoryOptions,
    data: IobDataEntry[],
    startTime: number,
): void {
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
        return adapter.sendTo(
            msg.from,
            msg.command,
            {
                result: [],
                error: data,
                sessionId: options.sessionId,
            },
            msg.callback,
        );
    }

    if (data[0] && data[1]) {
        // first | start          | afterFirst | ...... | last | end            | afterLast
        // 5     |                | 8          |  9 | 1 | 3    |                | 5
        //       | 5+(8-5/tsDiff) |            |  9 | 1 |      | 3+(5-3/tsDiff) |
        //       (9 - 6.5) + (4 - 1)

        if (data[1].ts === options.start) {
            data.splice(0, 1);
        }

        if (data[0].ts < options.start! && data[0].val! > data[1].val!) {
            data.splice(0, 1);
        }

        // interpolate from first to start time
        if (data[0].ts < options.start!) {
            const val =
                data[0].val! +
                (data[1].val! - data[0].val!) * ((options.start! - data[0].ts) / (data[1].ts - data[0].ts));
            data.splice(0, 1);
            data.unshift({ ts: options.start!, val, i: true });
        }

        if (data[data.length - 2] !== undefined && data[data.length - 2].ts === options.end) {
            data.length--;
        }

        const veryLast = data[data.length - 1];
        const beforeLast = data[data.length - 2];

        // interpolate from end time to last
        if (veryLast !== undefined && beforeLast !== undefined && options.end! < veryLast.ts) {
            const val =
                beforeLast.val! +
                (veryLast.val! - beforeLast.val!) * ((options.end! - beforeLast.ts) / (veryLast.ts - beforeLast.ts));
            data.length--;
            data.push({ ts: options.end!, val, i: true });
        }

        // at this point we expect [6.5, 9, 1, 4]
        // at this point we expect [150, 200, 500, 0, 400, 0, 50]
        let sum = 0;
        if (data.length > 1) {
            let val = data[data.length - 1].val!;
            for (let i = data.length - 2; i >= 0; i--) {
                if (data[i].val! < val) {
                    sum += val - data[i].val!;
                }
                val = data[i].val!;
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

/**
 * Get quantile value from an array.
 *
 * @param q - quantile
 * @param list - list of sorted values (ascending)
 * @returns Quantile value
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
 *
 * @template T
 * @param q - quantile or a list of quantiles
 * @param list - array of values
 */
function quantile(q: number, list: number[]): number {
    list = list.slice().sort(function (a, b) {
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

    return getQuantileValue(q, list);
}

export function sortByTs(a: IobDataEntry, b: IobDataEntry): number {
    return a.ts - b.ts;
}
