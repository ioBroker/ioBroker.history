'use strict';
// todo     add cache data
// todo     error tests
// todo     clean up

const fs = require('fs');
const Aggregate = require(`${__dirname}/aggregate.js`);

let gOptions;

if (typeof module === 'undefined' || !module || !module.parent) {
    gOptions = JSON.parse(process.argv[2]);

    if (gOptions && gOptions.debugLog) {
        gOptions.log = (...args) => {
            process.send && process.send(['debug', ...args]);
        }
    } else {
        gOptions.log = () => {};
    }
}

let finished = false;
let initialized = false;
let cacheReceived = false;
let cacheData = null;

if (typeof module === 'undefined' || !module || !module.parent) {
    process.on('message', msg => {
        if (msg[0] === 'cacheData') {
            cacheReceived = true;
            gOptions.log(`${Date.now()}: cacheData received (initialized=${initialized})`);
            if (initialized) {
                gOptions.log(`${Date.now()}: aggregate cacheData`);
                msg[1] && Aggregate.aggregation(gOptions, msg[1]);
                gOptions.log(`${Date.now()}: aggregate cacheData Done`);

                if (finished) {
                    response(gOptions);
                } else {
                    finished = true;
                }
            } else {
                cacheData = msg[1];
            }
        } else if (msg[0] === 'exit') {
            process.exit();
        }
    });
}

function getDirectories(path) {
    return fs.readdirSync(path)
        .filter(file =>
            !file.startsWith('.') && fs.statSync(`${path}/${file}`).isDirectory());
}

function tsSort(a, b) {
    return b.ts - a.ts;
}

function getFilenameForID(path, date, id) {
    if (typeof id !== 'string') {
        id = id.toString();
    }
    const safeId = id.replace(/[\u0000|*,;"'<>?:\/\\]/g, '~');
    return `${path}${date.toString()}/history.${safeId}.json`;
}

function getFileData(options) {
    const addId = options.addId;
    const dayStart = parseInt(ts2day(options.start), 10);
    const dayEnd   = parseInt(ts2day(options.end), 10);
    let fileData   = [];

    options.log(`${Date.now()}: getFileData start ${dayStart} end ${dayEnd}`);
    // get list of directories
    let dayList = getDirectories(options.path);
    if (options.returnNewestEntries) {
        dayList = dayList.sort((a, b) => b - a)
    } else {
        dayList = dayList.sort((a, b) => a - b)
    }

    // get all files in directory
    for (let i = 0; i < dayList.length; i++) {
        const day = parseInt(dayList[i], 10);

        if (!isNaN(day) && day >= dayStart && day <= dayEnd) {
            options.log(`${Date.now()}: getFileData process ${day}`);
            const file = getFilenameForID(options.path, dayList[i], options.id);
            const tsCheck = new Date(Math.floor(day/10000),0, 1).getTime();

            try {
                if (fs.existsSync(file)) {
                    try {
                        fileData = JSON.parse(fs.readFileSync(file, 'utf-8')).sort(tsSort);
                    } catch (e) {
                        fileData = null;
                    }
                    if (fileData) {
                        let last = false;
                        for (const ii in fileData) {
                            if (!fileData.hasOwnProperty(ii)) {
                                continue;
                            }

                            // if a ts in seconds is in then convert on the fly
                            if (fileData[ii].ts && fileData[ii].ts < tsCheck) {
                                fileData[ii].ts *= 1000;
                            }

                            if (typeof fileData[ii].val === 'number' && isFinite(fileData[ii].val) && options.round) {
                                fileData[ii].val = Math.round(fileData[ii].val * options.round) / options.round;
                            }
                            if (options.ack) {
                                fileData[ii].ack = !!fileData[ii].ack;
                            }
                            if (addId) {
                                fileData[ii].id = options.id;
                            }
                            if ((options.returnNewestEntries || options.aggregate === 'onchange' || options.aggregate === '' || options.aggregate === 'none') && data.length >= options.count) {
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
                    options.log(`${Date.now()}: getFileData aggregate ${day} (length=${fileData && fileData.length})`);
                    fileData && Aggregate.aggregation(options, fileData);
                }
            } catch (err) {
                options.log(`${Date.now()}: getFileData error ${day} ${err.message}`);
                options.log(`${Date.now()}: ${err.stack}`);
            }
        }
        if (day > dayEnd) {
            break;
        }
    }
    options.log(`${Date.now()}: getFileData Done`);
}

function ts2day(ts) {
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

function response(options) {
    options.log(`${Date.now()}: Finish Aggregation`);
    Aggregate.finishAggregation(options);
    options.log(`${Date.now()}: Send Response`);
    if (typeof module === 'undefined' || !module || !module.parent) {
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

function processData(options) {
    options.log(`${Date.now()}: Initialize structures`);
    Aggregate.initAggregate(options);
    initialized = true;

    if (cacheReceived) {
        options.log(`${Date.now()}: Aggregate cacheData`);
        cacheData && Aggregate.aggregation(options, cacheData);
        options.log(`${Date.now()}: Aggregate cacheData Done`);
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

if (typeof module === 'undefined' || !module || !module.parent) {
    gOptions.log(`${Date.now()}: Initialize getHistory`);

    if (process.send) {
        gOptions.log(`${Date.now()}: Request cacheData`);
        process.send(['getCache', gOptions], () => processData(gOptions));
    } else {
        finished = true;
        processData(gOptions);
    }
}

if (typeof module !== 'undefined' && module.parent) {
    module.exports.initAggregate     = Aggregate.initAggregate;
    module.exports.aggregation       = Aggregate.aggregation;
    module.exports.finishAggregation = Aggregate.finishAggregation;
    module.exports.getFileData       = getFileData;
    module.exports.ts2day            = ts2day;
    module.exports.response          = response;
    module.exports.getFilenameForID  = getFilenameForID;

    // how to use:
    // const options = initAggregate(options);
    // getFileData(options);
    // const result = response(options);
}
